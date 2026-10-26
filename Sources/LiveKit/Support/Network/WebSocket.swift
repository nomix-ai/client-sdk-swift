/*
 * Copyright 2026 LiveKit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation
import Network

// The signalling WebSocket over Network.framework instead of URLSession: Foundation keeps every URLSession that ran a
// WebSocket task, and the task, after it is cancelled and the session invalidated, about 25 KB a connection (macOS 27,
// iOS 26.5 simulator; a repro without LiveKit leaks the same, an NWConnection does not). Same interface as 2.18.0's.
/// The peer's close frame, carried as the `internalError` of the thrown ``LiveKitError``.
struct WebSocketClosedByPeer: Error, Sendable {
    /// The close code the peer sent; 1005 when it sent none.
    let code: UInt16

    init(_ code: NWProtocolWebSocket.CloseCode) {
        self.code = switch code {
        case let .protocolCode(code): code.rawValue
        case let .applicationCode(code), let .privateCode(code): code
        @unknown default: 1005
        }
    }
}

actor WebSocket: Loggable, AsyncSequence {
    typealias Element = URLSessionWebSocketTask.Message

    /// Not private: a test holds it weakly to see it freed.
    let connection: NWConnection
    private static let queue = DispatchQueue(label: "LiveKitSDK.webSocket")

    init(url: URL, token: String, connectOptions: ConnectOptions?) async throws {
        let webSocket = NWProtocolWebSocket.Options()
        webSocket.autoReplyPing = true
        webSocket.maximumMessageSize = 16 << 20
        webSocket.setAdditionalHeaders([("Authorization", "Bearer \(token)")])
        let timeout = connectOptions?.socketConnectTimeoutInterval ?? .defaultSocketConnect
        let tcp = NWProtocolTCP.Options()
        tcp.connectionTimeout = Int(timeout)
        let parameters = url.scheme == "wss" ? NWParameters(tls: NWProtocolTLS.Options(), tcp: tcp) : NWParameters(tls: nil, tcp: tcp)
        parameters.serviceClass = .responsiveData
        #if os(iOS) || os(visionOS)
        // As 2.18.0's URLSession configuration: the connection moves between Wi-Fi and cellular.
        parameters.multipathServiceType = .handover
        #endif
        parameters.defaultProtocolStack.applicationProtocols.insert(webSocket, at: 0)
        let connection = NWConnection(to: .url(url), using: parameters)
        self.connection = connection

        let opened = OpenOnce()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                opened.set(continuation)
                connection.stateUpdateHandler = { state in
                    switch state {
                    case .ready: opened.resume(nil)
                    case let .failed(error), let .waiting(error): opened.resume(LiveKitError(.network, internalError: error))
                    case .cancelled: opened.resume(LiveKitError(.cancelled))
                    default: break
                    }
                }
                connection.start(queue: Self.queue)
                // A peer that closes instead of answering the upgrade (a proxy whose upstream is down) leaves the
                // connection in .preparing for 60 s without a word; URLSession failed it at once.
                // Weakly: a socket closed before the deadline is not held until it.
                Self.queue.asyncAfter(deadline: .now() + timeout) { [weak connection] in
                    if opened.resume(LiveKitError(.timedOut, message: "WebSocket upgrade not answered")) { connection?.cancel() }
                }
            }
        } onCancel: {
            connection.cancel()
        }
    }

    deinit {
        connection.cancel()
    }

    nonisolated func close() {
        connection.cancel()
    }

    // MARK: - AsyncSequence

    struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate let connection: NWConnection

        /// Completions in a row without WebSocket metadata after which the stream counts as gone: a peer's bare TCP FIN
        /// gives one, and the receive after it fails (ENODATA); more than a few would be a stream that ended unsaid.
        static let emptyCompletionLimit = 3

        func next() async throws -> URLSessionWebSocketTask.Message? {
            var empty = 0
            while true {
                switch try await receive() {
                case let .message(message):
                    return message
                case .end:
                    return nil
                case .control:
                    empty = 0
                case .empty:
                    // A peer's bare TCP FIN arrives without metadata; the next receive fails on it, so the reconnect
                    // starts at once instead of after the ping timeout.
                    empty += 1
                    if empty >= Self.emptyCompletionLimit {
                        throw LiveKitError(.network, message: "WebSocket ended without a close frame")
                    }
                }
            }
        }

        private enum Frame {
            case message(URLSessionWebSocketTask.Message)
            /// Ping or pong: read past.
            case control
            /// A completion without WebSocket metadata.
            case empty
            /// Our own close.
            case end
        }

        private func receive() async throws -> Frame {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Frame, Error>) in
                    connection.receiveMessage { content, context, _, error in
                        if let error {
                            // A cancelled connection ends the sequence, as a closed URLSession task did.
                            if case .posix(.ECANCELED) = error { return continuation.resume(returning: .end) }
                            return continuation.resume(throwing: LiveKitError(.network, internalError: error))
                        }
                        let metadata = context?.protocolMetadata(definition: NWProtocolWebSocket.definition) as? NWProtocolWebSocket.Metadata
                        guard let metadata else { return continuation.resume(returning: .empty) }
                        switch metadata.opcode {
                        case .binary: continuation.resume(returning: .message(.data(content ?? Data())))
                        case .text: continuation.resume(returning: .message(.string(String(decoding: content ?? Data(), as: UTF8.self))))
                        case .close:
                            // The peer's close fails the stream, so the signal client reconnects at once, as the JS,
                            // Android and Rust SDKs do; only our own close ends it (upstream PR #1179).
                            let closed = WebSocketClosedByPeer(metadata.closeCode)
                            continuation.resume(throwing: LiveKitError(.network, message: "Closed by the peer (\(closed.code))",
                                                                       internalError: closed))
                        default: continuation.resume(returning: .control)
                        }
                    }
                }
            } onCancel: {
                connection.cancel()
            }
        }
    }

    nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(connection: connection)
    }

    // MARK: - Send

    nonisolated func send(data: Data) async throws {
        let context = NWConnection.ContentContext(identifier: "binary",
                                                  metadata: [NWProtocolWebSocket.Metadata(opcode: .binary)])
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            connection.send(content: data, contentContext: context, isComplete: true, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(throwing: LiveKitError(.network, internalError: error))
                } else {
                    continuation.resume()
                }
            })
        }
    }

    /// The connect continuation, resumed once whatever the connection reports after.
    private final class OpenOnce: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Void, Error>?

        func set(_ continuation: CheckedContinuation<Void, Error>) {
            lock.lock()
            self.continuation = continuation
            lock.unlock()
        }

        /// Whether this was the first outcome, the one the opening got.
        @discardableResult
        func resume(_ error: Error?) -> Bool {
            lock.lock()
            let continuation = self.continuation
            self.continuation = nil
            lock.unlock()
            if let error { continuation?.resume(throwing: error) } else { continuation?.resume() }
            return continuation != nil
        }
    }
}

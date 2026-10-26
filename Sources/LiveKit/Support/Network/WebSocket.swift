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

        func next() async throws -> URLSessionWebSocketTask.Message? {
            while true {
                guard let frame = try await receive() else { return nil }
                if let message = frame { return message }
                // Not a message: a control frame, or a peer's bare TCP FIN, which arrives without metadata; the next
                // receive fails on it (ENODATA), so the reconnect starts at once instead of after the ping timeout.
            }
        }

        /// nil at the end of the stream, `.some(nil)` for a frame that is not a message.
        private func receive() async throws -> URLSessionWebSocketTask.Message?? {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<URLSessionWebSocketTask.Message??, Error>) in
                    connection.receiveMessage { content, context, _, error in
                        if let error {
                            // A cancelled connection ends the sequence, as a closed URLSession task did.
                            if case .posix(.ECANCELED) = error { return continuation.resume(returning: nil) }
                            return continuation.resume(throwing: LiveKitError(.network, internalError: error))
                        }
                        let metadata = context?.protocolMetadata(definition: NWProtocolWebSocket.definition) as? NWProtocolWebSocket.Metadata
                        switch metadata?.opcode {
                        case .binary?: continuation.resume(returning: .some(.data(content ?? Data())))
                        case .text?: continuation.resume(returning: .some(.string(String(decoding: content ?? Data(), as: UTF8.self))))
                        case .close?: continuation.resume(returning: nil)
                        default: continuation.resume(returning: .some(nil))
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

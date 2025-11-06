import Foundation
import Core
import Persistence

struct ThematicClassifier {
    private let config: AIConfig

    init(config: AIConfig) {
        self.config = config
    }

    func classify(imageData: Data, albums: [ThematicAlbum]) throws -> [String] {
        guard !albums.isEmpty else { return [] }
        let base64 = imageData.base64EncodedString()
        let formattedAlbums = albums.enumerated().map { index, album in
            if let description = album.description, !description.isEmpty {
                return "\(index + 1). \(album.name) — \(description)"
            } else {
                return "\(index + 1). \(album.name)"
            }
        }.joined(separator: "\n")
        let systemPrompt = "You are an expert photography curator. You categorize photos into user-provided thematic albums. Always respond with valid JSON like {\"albums\":[\"Name\"]}. Use only the supplied album names and return an empty array if none apply. Not every photo is expected to fit into a collection, and that's okay."
        let userPrompt = "Which of the following collections does this photo belong in? Only provide a match when you are very confident the photo is a strong match for the collection. Respond ONLY with JSON of the matching album names. Albums:\n\(formattedAlbums)"

        let payload = ChatCompletionRequest(
            model: config.model,
            messages: [
                .init(role: "system", content: [.text(systemPrompt)]),
                .init(role: "user", content: [.text(userPrompt), .image(base64)])
            ]
        )

        let requestData = try JSONEncoder().encode(payload)
        let endpoint = config.baseURL.appendingPathComponent("v1/chat/completions")
        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.addValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = requestData

        let responseData = try synchronousRequest(request: request)
        let decoder = JSONDecoder()
        guard let completion = try? decoder.decode(ChatCompletionResponse.self, from: responseData) else {
            if let apiError = try? decoder.decode(AIErrorResponse.self, from: responseData) {
                throw ExportError.invalidArgument("AI error: \(apiError.error.message)")
            }
            if let text = String(data: responseData, encoding: .utf8) {
                throw ExportError.invalidArgument("Unexpected AI response: \(text)")
            }
            throw ExportError.invalidArgument("Unexpected AI response (binary)")
        }
        guard let rawText = completion.choices.first?.message.content else {
            throw ExportError.invalidArgument("AI response missing content")
        }
        if let data = rawText.data(using: .utf8), let parsed = try? JSONDecoder().decode(AlbumResponse.self, from: data) {
            let allowed = Set(albums.map { $0.name })
            return parsed.albums.filter { allowed.contains($0) }
        }
        throw ExportError.invalidArgument("Unexpected AI response: \(rawText)")
    }

    private func synchronousRequest(request: URLRequest) throws -> Data {
        let semaphore = DispatchSemaphore(value: 0)
        let resultBox = ResultBox<Result<Data, Error>>()
        URLSession.shared.dataTask(with: request) { data, _, error in
            if let data {
                resultBox.value = .success(data)
            } else if let error {
                resultBox.value = .failure(error)
            } else {
                resultBox.value = .failure(ExportError.invalidArgument("No response from AI service"))
            }
            semaphore.signal()
        }.resume()
        semaphore.wait()
        guard let result = resultBox.value else {
            throw ExportError.invalidArgument("No result from AI service")
        }
        switch result {
        case .success(let data):
            return data
        case .failure(let error):
            throw error
        }
    }

    private struct ChatCompletionRequest: Encodable {
        let model: String
        let messages: [Message]

        struct Message: Encodable {
            let role: String
            let content: [Content]
        }

        struct Content: Encodable {
            let type: String
            let text: String?
            let imageURL: ImageURL?

            enum CodingKeys: String, CodingKey {
                case type
                case text
                case imageURL = "image_url"
            }

            struct ImageURL: Encodable {
                let url: String
            }

            static func text(_ value: String) -> Content {
                Content(type: "text", text: value, imageURL: nil)
            }

            static func image(_ base64: String) -> Content {
                let dataURL = "data:image/jpeg;base64,\(base64)"
                return Content(type: "image_url", text: nil, imageURL: ImageURL(url: dataURL))
            }
        }
    }

    private struct ChatCompletionResponse: Decodable {
        let choices: [Choice]

        struct Choice: Decodable {
            let message: Message
        }

        struct Message: Decodable {
            let content: String?
        }
    }

    private struct AlbumResponse: Decodable {
        let albums: [String]
    }

    private struct AIErrorResponse: Decodable {
        let error: ErrorInfo

        struct ErrorInfo: Decodable {
            let message: String
        }
    }
}

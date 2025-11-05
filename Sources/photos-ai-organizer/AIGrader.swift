import Foundation
import UniformTypeIdentifiers
import Persistence
import Core

struct AIGrader {
    private let config: AIConfig

    init(config: AIConfig) {
        self.config = config
    }

    func grade(imageData: Data) throws -> Int {
        let base64 = imageData.base64EncodedString()
        let systemPrompt = "You are an expert photo curator. Return JSON like {\"grade\":7} with an integer 0-10 grade."
        let userText = "Consider composition, exposure, sharpness, artistry, and overall image quality. Score 0-10 (10 museum quality)."

        let payload = ChatCompletionRequest(
            model: config.model,
            messages: [
                .init(role: "system", content: [.text(systemPrompt)]),
                .init(role: "user", content: [.text(userText), .image(base64)])
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
            throw ExportError.invalidConfig("AI response missing content")
        }
        if let data = rawText.data(using: .utf8),
           let gradePayload = try? JSONDecoder().decode(AIGrade.self, from: data) {
            return gradePayload.grade
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
            let imageBase64: String?

            enum CodingKeys: String, CodingKey {
                case type
                case text
                case imageBase64 = "image_base64"
            }

            static func text(_ value: String) -> Content {
                Content(type: "text", text: value, imageBase64: nil)
            }

            static func image(_ base64: String) -> Content {
                Content(type: "input_image", text: nil, imageBase64: base64)
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

    private struct AIGrade: Decodable {
        let grade: Int
    }

    private struct AIErrorResponse: Decodable {
        let error: ErrorInfo

        struct ErrorInfo: Decodable {
            let message: String
        }
    }
}

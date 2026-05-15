package com.connecthub.translationservice.service.provider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@Order(1)
@Slf4j
public class GeminiTranslationProvider implements TranslationProvider {

    private static final String PROVIDER = "gemini";
    private static final String GENERATE_CONTENT_SUFFIX = ":generateContent";
    private static final String SYSTEM_INSTRUCTION = """
            You are a translation assistant.
            Correct obvious spelling mistakes and typos in the source text before translating it.
            Preserve the original meaning, tone, links, names, emojis, and punctuation.
            If the target language is the same as the source language, return the typo-corrected text in that language.
            Return only JSON that matches the provided schema.
            """;
    private static final Map<String, Object> RESPONSE_SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of(
                    "correctedText", Map.of(
                            "type", "string",
                            "description", "Source text after correcting obvious typos."
                    ),
                    "translatedText", Map.of(
                            "type", "string",
                            "description", "Translated text in the requested target language."
                    ),
                    "sourceLanguage", Map.of(
                            "type", "string",
                            "description", "Detected or applied source language code."
                    )
            ),
            "required", List.of("correctedText", "translatedText", "sourceLanguage")
    );

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final boolean enabled;
    private final String apiUrl;
    private final String model;
    private final String apiKey;

    public GeminiTranslationProvider(@Qualifier("translationRestTemplate") RestTemplate restTemplate,
                                     ObjectMapper objectMapper,
                                     @Value("${translation.gemini.enabled:true}") boolean enabled,
                                     @Value("${translation.gemini.url:https://generativelanguage.googleapis.com/v1beta/models}") String apiUrl,
                                     @Value("${translation.gemini.model:gemini-2.5-flash}") String model,
                                     @Value("${translation.gemini.key:}") String apiKey) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.enabled = enabled;
        this.apiUrl = apiUrl;
        this.model = model;
        this.apiKey = apiKey;
    }

    @Override
    public Optional<TranslationProviderResult> translate(TranslationJob job) {
        if (!enabled || !StringUtils.hasText(apiKey) || !StringUtils.hasText(apiUrl) || !StringUtils.hasText(model)) {
            return Optional.empty();
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("x-goog-api-key", apiKey);

        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("system_instruction", Map.of(
                "parts", List.of(Map.of("text", SYSTEM_INSTRUCTION))
        ));
        requestBody.put("contents", List.of(Map.of(
                "parts", List.of(Map.of("text", buildPrompt(job)))
        )));
        requestBody.put("generationConfig", Map.of(
                "responseMimeType", "application/json",
                "responseJsonSchema", RESPONSE_SCHEMA
        ));

        try {
            ResponseEntity<Map> response = restTemplate.exchange(
                    resolveEndpoint(),
                    HttpMethod.POST,
                    new HttpEntity<>(requestBody, headers),
                    Map.class
            );

            Map<?, ?> responseBody = response.getBody();
            if (responseBody == null) {
                log.warn("Gemini returned an empty body");
                return Optional.empty();
            }

            String jsonPayload = extractCandidateText(responseBody);
            if (!StringUtils.hasText(jsonPayload)) {
                log.warn("Gemini returned no candidate text");
                return Optional.empty();
            }

            Map<String, Object> parsed = objectMapper.readValue(jsonPayload, new TypeReference<>() {
            });
            String correctedText = valueAsString(parsed.get("correctedText"));
            String translatedText = valueAsString(parsed.get("translatedText"));
            String sourceLanguage = valueAsString(parsed.get("sourceLanguage"));

            if (!StringUtils.hasText(translatedText)) {
                log.warn("Gemini returned an empty translated text payload");
                return Optional.empty();
            }

            return Optional.of(new TranslationProviderResult(
                    StringUtils.hasText(correctedText) ? correctedText : job.originalText(),
                    translatedText,
                    StringUtils.hasText(sourceLanguage) ? sourceLanguage : job.sourceLanguage(),
                    job.targetLanguage(),
                    PROVIDER
            ));
        } catch (HttpStatusCodeException ex) {
            log.warn("Gemini returned {}: {}", ex.getStatusCode(), ex.getResponseBodyAsString());
            return Optional.empty();
        } catch (RestClientException ex) {
            log.warn("Gemini request failed: {}", ex.getMessage());
            return Optional.empty();
        } catch (IOException ex) {
            log.warn("Gemini returned invalid JSON: {}", ex.getMessage());
            return Optional.empty();
        }
    }

    private String buildPrompt(TranslationJob job) {
        return """
                Translate the following text.
                source_language_hint: %s
                target_language: %s
                requirements:
                - Correct obvious typos in the source text before translating it.
                - Keep the original meaning, tone, numbers, links, and names.
                - Return sourceLanguage as a lowercase ISO 639-1 language code when possible.
                text:
                %s
                """.formatted(job.sourceLanguageHint(), job.targetLanguage(), job.originalText());
    }

    private String resolveEndpoint() {
        String normalizedUrl = apiUrl.trim();
        if (normalizedUrl.endsWith("/")) {
            normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - 1);
        }
        if (normalizedUrl.contains("{model}")) {
            normalizedUrl = normalizedUrl.replace("{model}", model);
        }
        if (normalizedUrl.endsWith(GENERATE_CONTENT_SUFFIX)) {
            return normalizedUrl;
        }
        return normalizedUrl + "/" + model + GENERATE_CONTENT_SUFFIX;
    }

    private String extractCandidateText(Map<?, ?> responseBody) {
        Object candidates = responseBody.get("candidates");
        if (!(candidates instanceof List<?> candidateList)) {
            return null;
        }

        for (Object candidate : candidateList) {
            if (!(candidate instanceof Map<?, ?> candidateMap)) {
                continue;
            }
            Object content = candidateMap.get("content");
            if (!(content instanceof Map<?, ?> contentMap)) {
                continue;
            }
            Object parts = contentMap.get("parts");
            if (!(parts instanceof List<?> partList)) {
                continue;
            }

            StringBuilder builder = new StringBuilder();
            for (Object part : partList) {
                if (part instanceof Map<?, ?> partMap) {
                    Object text = partMap.get("text");
                    if (text instanceof String textValue && StringUtils.hasText(textValue)) {
                        if (builder.length() > 0) {
                            builder.append('\n');
                        }
                        builder.append(textValue);
                    }
                }
            }

            if (builder.length() > 0) {
                return builder.toString();
            }
        }

        return null;
    }

    private String valueAsString(Object value) {
        return value instanceof String text ? text : null;
    }
}

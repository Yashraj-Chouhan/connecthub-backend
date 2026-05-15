package com.connecthub.websocketservice.service;

import com.connecthub.websocketservice.client.NotificationClient;
import com.connecthub.websocketservice.client.RoomClient;
import com.connecthub.websocketservice.client.MessageClient;
import com.connecthub.websocketservice.config.WebSocketProperties;
import com.connecthub.websocketservice.dto.ChatMessage;
import com.connecthub.websocketservice.dto.NotificationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Objects;

@Service
/**
 * Coordinates real-time chat events, Kafka publishing, room fan-out, and
 * notification delivery for connected users.
 */
@RequiredArgsConstructor
@Slf4j
public class ChatService {

    private static final String EVENT_TYPE_TYPING = "TYPING_INDICATOR";
    private static final String EVENT_TYPE_REACTION = "REACTION";
    private static final String EVENT_TYPE_READ_RECEIPT = "READ_RECEIPT";
    private static final String EVENT_TYPE_MESSAGE_TRANSLATED = "MESSAGE_TRANSLATED";
    private static final String EVENT_TYPE_MESSAGE_TRANSCRIBED = "MESSAGE_TRANSCRIBED";
    private static final String EVENT_TYPE_CALL_PREFIX = "CALL_";
    private static final String DEFAULT_MESSAGE_TYPE = "TEXT";
    private static final String EVENT_TYPE_NOTIFICATION = "NOTIFICATION";

    private final SimpMessagingTemplate messagingTemplate;
    private final MessageClient messageClient;
    private final RoomClient roomClient;
    private final NotificationClient notificationClient;
    private final WebSocketProperties properties;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Entry point for all incoming WebSocket chat payloads. Depending on the
     * event type, the payload is either broadcast immediately or sent into the
     * Kafka-backed persistence flow.
     */
    public void handleMessage(ChatMessage message) {
        ChatMessage sanitized = sanitize(message);
        String eventType = sanitized.getEventType();

        if (!hasRoutingInfo(sanitized)) {
            log.warn("Ignoring malformed websocket payload without sender or room id");
            return;
        }

        if (EVENT_TYPE_TYPING.equals(eventType)) {
            handleTyping(sanitized);
            return;
        }

        if (EVENT_TYPE_REACTION.equals(eventType)) {
            handleReaction(sanitized);
            return;
        }

        if (EVENT_TYPE_READ_RECEIPT.equals(eventType)) {
            handleReadReceipt(sanitized);
            return;
        }

        if (EVENT_TYPE_MESSAGE_TRANSLATED.equals(eventType)) {
            if (sanitized.getMessageId() == null) {
                log.warn("Ignoring translated message event without a message id for room {}", sanitized.getRoomId());
                return;
            }

            broadcastExistingMessage(sanitized);
            return;
        }

        if (EVENT_TYPE_MESSAGE_TRANSCRIBED.equals(eventType)) {
            if (sanitized.getMessageId() == null) {
                log.warn("Ignoring transcribed message event without a message id for room {}", sanitized.getRoomId());
                return;
            }

            broadcastExistingMessage(sanitized);
            return;
        }

        if (isCallEvent(sanitized)) {
            broadcastExistingMessage(sanitized);
            return;
        }

        if (sanitized.getMessageId() != null) {
            broadcastExistingMessage(sanitized);
            return;
        }

        if (isFileMessageWithoutPersistenceMarker(sanitized)) {
            log.warn("Ignoring file websocket message without a persisted message id for room {}", sanitized.getRoomId());
            return;
        }

        persistAndBroadcast(sanitized);
    }

    public void handleTyping(ChatMessage message) {
        ChatMessage sanitized = sanitize(message);
        if (!hasRoutingInfo(sanitized)) {
            log.warn("Ignoring malformed typing payload");
            return;
        }
        sanitized.setEventType(EVENT_TYPE_TYPING);
        sanitized.setTimestamp(sanitized.getTimestamp() == null ? LocalDateTime.now() : sanitized.getTimestamp());
        broadcastToRoom(sanitized);
    }

    public void handleReadReceipt(ChatMessage message) {
        ChatMessage sanitized = sanitize(message);
        if (!hasRoutingInfo(sanitized)) {
            log.warn("Ignoring malformed read receipt payload");
            return;
        }
        sanitized.setEventType(EVENT_TYPE_READ_RECEIPT);
        sanitized.setTimestamp(sanitized.getTimestamp() == null ? LocalDateTime.now() : sanitized.getTimestamp());
        broadcastToRoom(sanitized);
    }

    private void handleReaction(ChatMessage message) {
        if (!hasRoutingInfo(message)) {
            log.warn("Ignoring malformed reaction payload");
            return;
        }

        if (message.getMessageId() == null || !StringUtils.hasText(message.getEmoji())) {
            log.warn("Ignoring malformed reaction payload for room {}", message.getRoomId());
            return;
        }

        message.setTimestamp(message.getTimestamp() == null ? LocalDateTime.now() : message.getTimestamp());
        broadcastToRoom(message);
    }

    /**
     * Publishes a new chat message to Kafka so message-service can persist it
     * before websocket-service broadcasts the durable record.
     */
    private void persistAndBroadcast(ChatMessage message) {
        String content = firstNonBlank(message.getContent(), message.getOriginalContent());
        if (!StringUtils.hasText(content)) {
            log.warn("Ignoring empty websocket chat message for room {}", message.getRoomId());
            return;
        }

        message.setContent(content);
        message.setOriginalContent(firstNonBlank(message.getOriginalContent(), content));
        message.setMessageType(normalizeMessageType(message.getMessageType()));
        message.setEventType(null);
        message.setDeleted(Boolean.FALSE);

        try {
            kafkaTemplate.send("chat.message.incoming", message);
            log.info("Sent message to kafka for persistence, room {}", message.getRoomId());
        } catch (Exception ex) {
            log.error("Failed to publish websocket message to Kafka for room {} from sender {}", message.getRoomId(), message.getSender(), ex);
        }
    }

    /**
     * Runs after message-service has persisted the chat event and published the
     * durable record back to Kafka for fan-out.
     */
    public void handleBroadcastMessage(ChatMessage message) {
        if (message == null) {
            log.warn("Ignoring null broadcast message from Kafka");
            return;
        }
        log.info("Received broadcast message from Kafka for room {}", message.getRoomId());
        ChatMessage broadcast = normalizeBroadcastMessage(message, message);
        if (!canBroadcast(broadcast)) {
            log.warn("Ignoring broadcast message without a room id");
            return;
        }
        broadcastToRoomAndSender(broadcast);
        notifyRoomMembers(broadcast);
    }

    public void handleTranscriptMessage(ChatMessage message) {
        if (message == null) {
            log.warn("Ignoring null transcript update from Kafka");
            return;
        }
        log.info("Received transcript update from Kafka for room {}", message.getRoomId());
        ChatMessage sanitized = sanitize(message);
        sanitized.setEventType(EVENT_TYPE_MESSAGE_TRANSCRIBED);
        broadcastExistingMessage(sanitized);
    }

    private void broadcastExistingMessage(ChatMessage message) {
        ChatMessage broadcast = normalizeBroadcastMessage(message, message);
        if (!canBroadcast(broadcast)) {
            log.warn("Ignoring broadcast candidate without a room id");
            return;
        }
        if (isCallEvent(broadcast)) {
            broadcastCallSignal(broadcast);
        } else {
            broadcastToRoomAndSender(broadcast);
        }
        if (isNotificationEligible(broadcast)) {
            notifyRoomMembers(broadcast);
        }
    }

    private void broadcastToRoom(ChatMessage message) {
        ChatMessage broadcast = normalizeBroadcastMessage(message, message);
        if (broadcast == null || !StringUtils.hasText(broadcast.getRoomId())) {
            log.warn("Ignoring room broadcast without a room id");
            return;
        }
        messagingTemplate.convertAndSend(roomDestination(broadcast.getRoomId()), broadcast);
    }

    private void broadcastToRoomAndSender(ChatMessage message) {
        ChatMessage broadcast = normalizeBroadcastMessage(message, message);
        if (!canBroadcast(broadcast)) {
            log.warn("Ignoring room and user broadcast without a room id");
            return;
        }
        messagingTemplate.convertAndSend(roomDestination(broadcast.getRoomId()), broadcast);
        if (StringUtils.hasText(broadcast.getSender())) {
            messagingTemplate.convertAndSend(userDestination(broadcast.getSender()), broadcast);
        }
        if (StringUtils.hasText(broadcast.getRecipientId())
                && !Objects.equals(broadcast.getRecipientId(), broadcast.getSender())) {
            messagingTemplate.convertAndSend(userDestination(broadcast.getRecipientId()), broadcast);
        }
    }

    private void broadcastCallSignal(ChatMessage message) {
        ChatMessage broadcast = normalizeBroadcastMessage(message, message);
        if (broadcast == null || !StringUtils.hasText(broadcast.getSender())) {
            log.warn("Ignoring call signal without a sender");
            return;
        }

        messagingTemplate.convertAndSend(userDestination(broadcast.getSender()), broadcast);

        if (StringUtils.hasText(broadcast.getRecipientId())
                && !Objects.equals(broadcast.getRecipientId(), broadcast.getSender())) {
            messagingTemplate.convertAndSend(userDestination(broadcast.getRecipientId()), broadcast);
        }
    }

    private ChatMessage normalizeBroadcastMessage(ChatMessage message, ChatMessage fallback) {
        if (message == null) {
            return fallback;
        }

        String originalContent = firstNonBlank(message.getOriginalContent(), message.getContent(), fallback == null ? null : fallback.getOriginalContent(), fallback == null ? null : fallback.getContent());
        String translatedContent = firstNonBlank(message.getTranslatedContent(), fallback == null ? null : fallback.getTranslatedContent());

        message.setOriginalContent(originalContent);
        message.setTranslatedContent(translatedContent);
        message.setContent(firstNonBlank(translatedContent, message.getContent(), fallback == null ? null : fallback.getContent(), originalContent));
        message.setMessageType(normalizeMessageType(firstNonBlank(message.getMessageType(), fallback == null ? null : fallback.getMessageType())));
        message.setEventType(normalizeEventType(firstNonBlank(message.getEventType(), fallback == null ? null : fallback.getEventType())));
        message.setTimestamp(message.getTimestamp() == null
                ? (fallback != null && fallback.getTimestamp() != null ? fallback.getTimestamp() : LocalDateTime.now())
                : message.getTimestamp());
        message.setDeleted(Boolean.TRUE.equals(message.getDeleted()));
        return message;
    }

    private ChatMessage sanitize(ChatMessage message) {
        if (message == null) {
            return new ChatMessage();
        }

        message.setSender(trimToNull(message.getSender()));
        message.setRoomId(trimToNull(message.getRoomId()));
        message.setContent(trimToNull(message.getContent()));
        message.setOriginalContent(trimToNull(message.getOriginalContent()));
        message.setTranslatedContent(trimToNull(message.getTranslatedContent()));
        message.setMessageType(normalizeMessageType(message.getMessageType()));
        message.setEventType(normalizeEventType(message.getEventType()));
        message.setEmoji(trimToNull(message.getEmoji()));
        message.setRecipientId(trimToNull(message.getRecipientId()));
        message.setDetectedLanguage(trimToNull(message.getDetectedLanguage()));
        message.setTranscript(trimToNull(message.getTranscript()));
        message.setTranscriptSourceLanguage(trimToNull(message.getTranscriptSourceLanguage()));

        return message;
    }

    private boolean isFileMessageWithoutPersistenceMarker(ChatMessage message) {
        return message.getMessageId() == null && "FILE".equalsIgnoreCase(message.getMessageType());
    }

    private boolean hasRoutingInfo(ChatMessage message) {
        return message != null
                && StringUtils.hasText(message.getRoomId())
                && StringUtils.hasText(message.getSender());
    }

    private boolean canBroadcast(ChatMessage message) {
        return message != null && StringUtils.hasText(message.getRoomId());
    }

    private String normalizeMessageType(String messageType) {
        if (!StringUtils.hasText(messageType)) {
            return DEFAULT_MESSAGE_TYPE;
        }
        return messageType.trim().toUpperCase(Locale.ROOT);
    }

    private String normalizeEventType(String eventType) {
        if (!StringUtils.hasText(eventType)) {
            return null;
        }
        return eventType.trim().toUpperCase(Locale.ROOT);
    }

    private String trimToNull(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return value.trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }

        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }

        return null;
    }

    private String roomDestination(String roomId) {
        return properties.getRoomTopicPrefix() + "/" + roomId;
    }

    private String userDestination(String userId) {
        return properties.getUserTopicPrefix() + "/" + userId;
    }

    private boolean isNotificationEligible(ChatMessage message) {
        return message != null
                && StringUtils.hasText(message.getRoomId())
                && StringUtils.hasText(message.getSender())
                && !StringUtils.hasText(message.getEventType());
    }

    private boolean isCallEvent(ChatMessage message) {
        return message != null
                && StringUtils.hasText(message.getEventType())
                && message.getEventType().startsWith(EVENT_TYPE_CALL_PREFIX);
    }

    private void notifyRoomMembers(ChatMessage message) {
        try {
            List<Map<String, Object>> members = roomClient.getMembers(message.getRoomId());
            Map<String, Object> room = roomClient.getRoom(message.getRoomId());
            String roomName = valueAsString(room == null ? null : room.get("name"));
            String notificationText = buildNotificationText(message, roomName);

            for (Map<String, Object> member : members) {
                String recipientId = valueAsString(member == null ? null : member.get("userId"));
                if (!StringUtils.hasText(recipientId) || Objects.equals(recipientId, message.getSender())) {
                    continue;
                }

                ChatMessage notification = ChatMessage.builder()
                        .messageId(message.getMessageId())
                        .sender(message.getSender())
                        .roomId(message.getRoomId())
                        .recipientId(recipientId)
                        .content(notificationText)
                        .originalContent(notificationText)
                        .messageType(EVENT_TYPE_NOTIFICATION)
                        .eventType(EVENT_TYPE_NOTIFICATION)
                        .timestamp(LocalDateTime.now())
                        .build();

                messagingTemplate.convertAndSend(userDestination(recipientId), notification);

                try {
                    notificationClient.createNotification(NotificationRequest.builder()
                            .userId(recipientId)
                            .message(notificationText)
                            .build());
                } catch (Exception notificationError) {
                    log.debug("Notification persistence failed for recipient {} in room {}", recipientId, message.getRoomId(), notificationError);
                }
            }
        } catch (Exception ex) {
            log.debug("Notification fan-out failed for room {}", message.getRoomId(), ex);
        }
    }

    private String buildNotificationText(ChatMessage message, String roomName) {
        String preview = firstNonBlank(
                message.getTranslatedContent(),
                message.getContent(),
                message.getOriginalContent()
        );
        if (!StringUtils.hasText(preview) && StringUtils.hasText(message.getAttachmentName())) {
            preview = "sent an attachment";
        }
        if (!StringUtils.hasText(preview)) {
            preview = "New message";
        }
        if (preview.length() > 120) {
            preview = preview.substring(0, 117) + "...";
        }

        String roomLabel = StringUtils.hasText(roomName) ? roomName : "room " + message.getRoomId();
        return roomLabel + ": " + preview;
    }

    private String valueAsString(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}

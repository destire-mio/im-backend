package main

import (
	"context"
	"errors"
	"unicode/utf8"
)

type messageServiceErrorCode string

const (
	messageServiceErrorInvalidJSON         messageServiceErrorCode = "invalid_json"
	messageServiceErrorInvalidMessage      messageServiceErrorCode = "invalid_message"
	messageServiceErrorReceiverNotFound    messageServiceErrorCode = "receiver_not_found"
	messageServiceErrorIdempotencyConflict messageServiceErrorCode = "idempotency_conflict"
	messageServiceErrorUnavailable         messageServiceErrorCode = "unavailable"
	messageServiceErrorInternal            messageServiceErrorCode = "internal"
)

type messageServiceError struct {
	Code    messageServiceErrorCode
	Message string
	Cause   error
}

func (err *messageServiceError) Error() string {
	if err.Cause != nil {
		return err.Cause.Error()
	}
	return err.Message
}

type sendMessageCommand struct {
	SenderID        int64
	ReceiverID      int64
	ClientMessageID string
	Content         *string
}

type sendMessageResult struct {
	Message message
	Created bool
}

type messageSendingService interface {
	send(context.Context, sendMessageCommand) (sendMessageResult, *messageServiceError)
}

type messageService struct {
	store *messageStore
}

func newMessageService(store *messageStore) *messageService {
	return &messageService{store: store}
}

func (service *messageService) send(
	ctx context.Context,
	command sendMessageCommand,
) (sendMessageResult, *messageServiceError) {
	if !validOpaqueID(command.ClientMessageID) || command.ReceiverID <= 0 || command.Content == nil {
		return sendMessageResult{}, invalidMessageError("clientMessageId, receiverId and content are required")
	}
	contentLength := utf8.RuneCountInString(*command.Content)
	if contentLength == 0 {
		return sendMessageResult{}, invalidMessageError("content must contain at least one character")
	}
	if contentLength > maxMessageContentRunes {
		return sendMessageResult{}, invalidMessageError("content is too long")
	}

	tx, err := service.store.begin(ctx)
	if err != nil {
		return sendMessageResult{}, &messageServiceError{Code: messageServiceErrorUnavailable, Cause: err}
	}
	defer tx.rollback(ctx)

	existing, found, err := tx.findIdempotentMessage(ctx, command.SenderID, command.ClientMessageID)
	if err != nil {
		return sendMessageResult{}, internalMessageError(err)
	}
	if found {
		if !sameMessageRequest(existing, command) {
			return sendMessageResult{}, &messageServiceError{Code: messageServiceErrorIdempotencyConflict}
		}
		return sendMessageResult{Message: existing}, nil
	}

	lowUserID, highUserID := command.SenderID, command.ReceiverID
	if lowUserID > highUserID {
		lowUserID, highUserID = highUserID, lowUserID
	}
	conversationID, conversationCreated, err := tx.resolveDirectConversation(ctx, lowUserID, highUserID)
	if err != nil {
		return sendMessageResult{}, classifyMessageStoreError(err)
	}

	if conversationCreated {
		if err := tx.insertConversationMembers(ctx, conversationID, command.SenderID, command.ReceiverID); err != nil {
			return sendMessageResult{}, classifyMessageStoreError(err)
		}
	}

	created, inserted, err := tx.insertMessageAndOutbox(
		ctx,
		conversationID,
		command.SenderID,
		command.ReceiverID,
		command.ClientMessageID,
		*command.Content,
	)
	if err != nil {
		return sendMessageResult{}, internalMessageError(err)
	}
	if !inserted {
		// A concurrent retry won the idempotency race. Rolling back also
		// restores our provisional sequence allocation, so no gap is left.
		if err := tx.rollback(ctx); err != nil {
			return sendMessageResult{}, internalMessageError(err)
		}
		winner, err := service.store.findCommittedIdempotentMessage(ctx, command.SenderID, command.ClientMessageID)
		if err != nil {
			return sendMessageResult{}, internalMessageError(err)
		}
		if !sameMessageRequest(winner, command) {
			return sendMessageResult{}, &messageServiceError{Code: messageServiceErrorIdempotencyConflict}
		}
		return sendMessageResult{Message: winner}, nil
	}

	if err := tx.commit(ctx); err != nil {
		return sendMessageResult{}, internalMessageError(err)
	}
	return sendMessageResult{Message: created, Created: true}, nil
}

func sameMessageRequest(existing message, command sendMessageCommand) bool {
	return existing.ReceiverID == command.ReceiverID && existing.Content == *command.Content
}

func invalidMessageError(message string) *messageServiceError {
	return &messageServiceError{Code: messageServiceErrorInvalidMessage, Message: message}
}

func internalMessageError(cause error) *messageServiceError {
	return &messageServiceError{Code: messageServiceErrorInternal, Cause: cause}
}

func classifyMessageStoreError(err error) *messageServiceError {
	if errors.Is(err, errMessageReceiverNotFound) {
		return &messageServiceError{Code: messageServiceErrorReceiverNotFound, Cause: err}
	}
	return internalMessageError(err)
}

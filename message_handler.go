package main

import (
	"net/http"
)

func (app *application) createMessage(w http.ResponseWriter, r *http.Request) {
	var input createMessageRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeMessageAPIError(w, r, &messageServiceError{
			Code: messageServiceErrorInvalidJSON,
		})
		return
	}

	service := app.messageSender
	if service == nil {
		service = newMessageService(&messageStore{db: app.db})
	}
	result, serviceError := service.send(r.Context(), sendMessageCommand{
		SenderID:        authenticatedUserID(r.Context()),
		ReceiverID:      input.ReceiverID,
		ClientMessageID: input.ClientMessageID,
		Content:         input.Content,
	})
	if serviceError != nil {
		writeMessageAPIError(w, r, serviceError)
		return
	}

	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result.Message)
}

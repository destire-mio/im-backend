package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeMessageSendingService struct {
	result  sendMessageResult
	err     *messageServiceError
	command sendMessageCommand
	called  bool
}

func (service *fakeMessageSendingService) send(
	_ context.Context,
	command sendMessageCommand,
) (sendMessageResult, *messageServiceError) {
	service.called = true
	service.command = command
	return service.result, service.err
}

func TestCreateMessageHandlerDelegatesBusinessWorkflowToService(t *testing.T) {
	content := "hello"
	service := &fakeMessageSendingService{result: sendMessageResult{
		Message: message{ID: 42, SenderID: 7, ReceiverID: 8, Content: content},
		Created: true,
	}}
	app := &application{messageSender: service}
	request := httptest.NewRequest(
		http.MethodPost,
		"/messages",
		strings.NewReader(`{"clientMessageId":"client-1","receiverId":8,"content":"hello"}`),
	)
	request = request.WithContext(context.WithValue(request.Context(), authenticatedUserIDKey, int64(7)))
	recorder := httptest.NewRecorder()

	requestIDMiddleware(http.HandlerFunc(app.createMessage)).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", recorder.Code)
	}
	if !service.called {
		t.Fatal("message service was not called")
	}
	if service.command.SenderID != 7 || service.command.ReceiverID != 8 || service.command.ClientMessageID != "client-1" {
		t.Fatalf("unexpected service command: %+v", service.command)
	}
	if service.command.Content == nil || *service.command.Content != content {
		t.Fatalf("service content = %v, want %q", service.command.Content, content)
	}
}

func TestCreateMessageHandlerUsesStableErrorContract(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr *messageServiceError
		wantStatus int
		wantCode   string
	}{
		{
			name:       "invalid message",
			serviceErr: invalidMessageError("content is too long"),
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_MESSAGE",
		},
		{
			name:       "idempotency conflict",
			serviceErr: &messageServiceError{Code: messageServiceErrorIdempotencyConflict},
			wantStatus: http.StatusConflict,
			wantCode:   "CLIENT_MESSAGE_ID_CONFLICT",
		},
		{
			name:       "dependency unavailable",
			serviceErr: &messageServiceError{Code: messageServiceErrorUnavailable, Cause: errors.New("database unavailable")},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "SERVICE_UNAVAILABLE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeMessageSendingService{err: test.serviceErr}
			app := &application{messageSender: service}
			request := httptest.NewRequest(
				http.MethodPost,
				"/messages",
				strings.NewReader(`{"clientMessageId":"client-1","receiverId":8,"content":"hello"}`),
			)
			request = request.WithContext(context.WithValue(request.Context(), authenticatedUserIDKey, int64(7)))
			recorder := httptest.NewRecorder()

			requestIDMiddleware(http.HandlerFunc(app.createMessage)).ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			var response apiErrorResponse
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Code != test.wantCode {
				t.Fatalf("code = %q, want %q", response.Code, test.wantCode)
			}
			if response.RequestID == "" {
				t.Fatal("requestId is empty")
			}
			if response.RequestID != recorder.Header().Get("X-Request-ID") {
				t.Fatalf("body requestId = %q, header = %q", response.RequestID, recorder.Header().Get("X-Request-ID"))
			}
		})
	}
}

func TestCreateMessageHandlerRejectsInvalidJSONBeforeCallingService(t *testing.T) {
	service := &fakeMessageSendingService{}
	app := &application{messageSender: service}
	request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"receiverId":`))
	request = request.WithContext(context.WithValue(request.Context(), authenticatedUserIDKey, int64(7)))
	recorder := httptest.NewRecorder()

	requestIDMiddleware(http.HandlerFunc(app.createMessage)).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if service.called {
		t.Fatal("service was called for invalid JSON")
	}
	var response apiErrorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Code != "INVALID_JSON" {
		t.Fatalf("code = %q, want INVALID_JSON", response.Code)
	}
}

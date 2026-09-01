package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type requestIDContextKey struct{}

var requestIDFallbackCounter atomic.Uint64

type apiErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := newRequestID()
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	if requestID == "" {
		return newRequestID()
	}
	return requestID
}

func newRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}
	return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), requestIDFallbackCounter.Add(1))
}

func writeMessageAPIError(w http.ResponseWriter, r *http.Request, serviceError *messageServiceError) {
	status, code, message := messageAPIErrorContract(serviceError)
	var cause error
	if serviceError != nil {
		cause = serviceError.Cause
	}
	writeAPIError(w, r, status, code, message, cause)
}

func writeAPIError(
	w http.ResponseWriter,
	r *http.Request,
	status int,
	code string,
	message string,
	cause error,
) {
	requestID := requestIDFromContext(r.Context())
	if cause != nil {
		log.Printf("request_id=%s code=%s cause=%v", requestID, code, cause)
	}
	writeJSON(w, status, apiErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	})
}

func messageAPIErrorContract(serviceError *messageServiceError) (int, string, string) {
	if serviceError == nil {
		return http.StatusInternalServerError, "INTERNAL_ERROR", "could not create message"
	}

	switch serviceError.Code {
	case messageServiceErrorInvalidJSON:
		return http.StatusBadRequest, "INVALID_JSON", "invalid JSON body"
	case messageServiceErrorInvalidMessage:
		return http.StatusBadRequest, "INVALID_MESSAGE", serviceError.Message
	case messageServiceErrorReceiverNotFound:
		return http.StatusBadRequest, "RECEIVER_NOT_FOUND", "receiverId does not exist"
	case messageServiceErrorIdempotencyConflict:
		return http.StatusConflict, "CLIENT_MESSAGE_ID_CONFLICT", "clientMessageId was already used with different message data"
	case messageServiceErrorUnavailable:
		return http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "message service is temporarily unavailable"
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "could not create message"
	}
}

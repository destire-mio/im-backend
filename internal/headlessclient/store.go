package headlessclient

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	stateVersion = 1
	stateMagic   = "IMHC1"
)

type AuthState struct {
	SessionID             int64  `json:"sessionId"`
	DeviceID              string `json:"deviceId"`
	SessionExpiresAt      string `json:"sessionExpiresAt"`
	AccessToken           string `json:"accessToken"`
	AccessTokenExpiresAt  string `json:"accessTokenExpiresAt"`
	RefreshToken          string `json:"refreshToken"`
	RefreshTokenExpiresAt string `json:"refreshTokenExpiresAt"`
}

type PendingRefresh struct {
	RefreshToken   string `json:"refreshToken"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type Message struct {
	ID              int64  `json:"id"`
	ConversationID  int64  `json:"conversationId"`
	ConversationSeq int64  `json:"conversationSeq"`
	ClientMessageID string `json:"clientMessageId"`
	SenderID        int64  `json:"senderId"`
	ReceiverID      int64  `json:"receiverId"`
	Content         string `json:"content"`
	CreatedAt       string `json:"createdAt"`
}

type ConversationState struct {
	AppliedCursor int64              `json:"appliedCursor"`
	AckedCursor   int64              `json:"ackedCursor"`
	PendingACK    int64              `json:"pendingAck"`
	Messages      map[string]Message `json:"messages"`
}

type State struct {
	Version        int                           `json:"version"`
	Auth           *AuthState                    `json:"auth,omitempty"`
	PendingRefresh *PendingRefresh               `json:"pendingRefresh,omitempty"`
	Conversations  map[string]*ConversationState `json:"conversations"`
}

type StoreOption func(*FileStore)

func WithBeforeRename(hook func() error) StoreOption {
	return func(store *FileStore) {
		store.beforeRename = hook
	}
}

type FileStore struct {
	path         string
	aead         cipher.AEAD
	beforeRename func() error
	mu           sync.Mutex
}

func NewFileStore(path string, key []byte, options ...StoreOption) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("state path is required")
	}
	if len(key) != 32 {
		return nil, errors.New("state encryption key must contain 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create state cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create state AEAD: %w", err)
	}
	store := &FileStore{path: path, aead: aead}
	for _, option := range options {
		option(store)
	}
	return store, nil
}

func (store *FileStore) Snapshot() (State, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.load()
}

func (store *FileStore) Update(update func(*State) error) error {
	if update == nil {
		return errors.New("state update is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	state, err := store.load()
	if err != nil {
		return err
	}
	if err := update(&state); err != nil {
		return err
	}
	state.normalize()
	return store.persist(state)
}

func (store *FileStore) load() (State, error) {
	encoded, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		state := State{}
		state.normalize()
		return state, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("read client state: %w", err)
	}
	nonceSize := store.aead.NonceSize()
	if len(encoded) < len(stateMagic)+nonceSize || string(encoded[:len(stateMagic)]) != stateMagic {
		return State{}, errors.New("client state has an invalid envelope")
	}
	nonce := encoded[len(stateMagic) : len(stateMagic)+nonceSize]
	ciphertext := encoded[len(stateMagic)+nonceSize:]
	plaintext, err := store.aead.Open(nil, nonce, ciphertext, []byte(stateMagic))
	if err != nil {
		return State{}, fmt.Errorf("decrypt client state: %w", err)
	}
	var state State
	if err := json.Unmarshal(plaintext, &state); err != nil {
		return State{}, fmt.Errorf("decode client state: %w", err)
	}
	if state.Version != stateVersion {
		return State{}, fmt.Errorf("unsupported client state version %d", state.Version)
	}
	state.normalize()
	return state, nil
}

func (store *FileStore) persist(state State) error {
	plaintext, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode client state: %w", err)
	}
	nonce := make([]byte, store.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("create client state nonce: %w", err)
	}
	ciphertext := store.aead.Seal(nil, nonce, plaintext, []byte(stateMagic))
	encoded := make([]byte, 0, len(stateMagic)+len(nonce)+len(ciphertext))
	encoded = append(encoded, stateMagic...)
	encoded = append(encoded, nonce...)
	encoded = append(encoded, ciphertext...)

	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create client state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary client state: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect temporary client state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write temporary client state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary client state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary client state: %w", err)
	}
	if store.beforeRename != nil {
		if err := store.beforeRename(); err != nil {
			return err
		}
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace client state: %w", err)
	}
	removeTemporary = false
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open client state directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync client state directory: %w", err)
	}
	return nil
}

func (state *State) normalize() {
	state.Version = stateVersion
	if state.Conversations == nil {
		state.Conversations = make(map[string]*ConversationState)
	}
	for _, conversation := range state.Conversations {
		if conversation.Messages == nil {
			conversation.Messages = make(map[string]Message)
		}
	}
}

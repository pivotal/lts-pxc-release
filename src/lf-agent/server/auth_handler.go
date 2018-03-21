package server

import (
	"crypto/subtle"
	"lf-agent/config"
	"net/http"

	"github.com/unrolled/render"
)

type AuthHandler struct {
	renderer *render.Render
	config   config.Config
}

func NewAuthHandler(renderer *render.Render, cfg config.Config) *AuthHandler {
	return &AuthHandler{
		renderer: renderer,
		config:   cfg,
	}
}

func (ah *AuthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
	resp := &Response{
		Status: "Unauthorized",
	}

	user, pass, ok := req.BasicAuth()
	if !ok || !equalUsingConstantTimeCompare(user, ah.config.HttpUsername) || !equalUsingConstantTimeCompare(pass, ah.config.HttpPassword) {
		ah.renderer.JSON(w, http.StatusUnauthorized, resp)
		return
	}

	next(w, req)
}

func equalUsingConstantTimeCompare(a, b string) bool {
	x := []byte(a)
	y := []byte(b)
	return subtle.ConstantTimeCompare(x, y) == 1
}

package querysvc

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/index.html
var consoleFS embed.FS

func consoleHandler() http.Handler {
	staticFS, err := fs.Sub(consoleFS, "web")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "console assets unavailable", http.StatusInternalServerError)
		})
	}
	return http.FileServer(http.FS(staticFS))
}

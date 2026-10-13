package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/robertramos/developer-observability-stack/internal/alerting"
	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/errs"
)

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if mt := mediaType(r); mt != "application/json" && mt != "" {
		s.fail(w, r, errs.New(errs.KindUnsupported, "use application/json"))
		return false
	}
	body, err := s.readBody(w, r)
	if err != nil {
		s.fail(w, r, err)
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.fail(w, r, errs.Invalid("invalid JSON: %v", err))
		return false
	}
	return true
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	out := s.d.Alerts.Alerts(p.Tenant)
	if out == nil {
		out = []alerting.Alert{}
	}
	s.ok(w, out)
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	out := s.d.Alerts.Rules(p.Tenant)
	if out == nil {
		out = []alerting.RuleStatus{}
	}
	s.ok(w, map[string]any{"rules": out, "channels": s.d.Alerts.Channels()})
}

func (s *Server) putRule(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var rule alerting.Rule
	if !s.decodeJSON(w, r, &rule) {
		return
	}
	if err := s.d.Alerts.PutRule(p.Tenant, rule); err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, envelope{Status: "success", Data: rule})
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := s.d.Alerts.DeleteRule(p.Tenant, r.PathValue("name")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listSilences(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	out := s.d.Alerts.Silences(p.Tenant)
	if out == nil {
		out = []alerting.SilenceView{}
	}
	s.ok(w, out)
}

func (s *Server) createSilence(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var sil alerting.Silence
	if !s.decodeJSON(w, r, &sil) {
		return
	}
	if sil.ID != "" {
		s.fail(w, r, errs.Invalid("id is assigned by the server"))
		return
	}
	created, err := s.d.Alerts.AddSilence(p.Tenant, sil)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, envelope{Status: "success", Data: created})
}

func (s *Server) deleteSilence(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := s.d.Alerts.ExpireSilence(p.Tenant, r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

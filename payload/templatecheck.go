// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

var (
	// ErrUnknownTemplate is returned for a template instance whose
	// template_ref names no template definition of the edge node's NBIRTH
	// (Sparkplug 3.0.0 tck-id-topics-nbirth-templates,
	// tck-id-payloads-template-definition-nbirth).
	ErrUnknownTemplate = errors.New("sparkplugb: unknown template definition")
	// ErrInvalidTemplate is returned for a template that breaks a rule of
	// Sparkplug 3.0.0 section 6.4.13 (Template) or 6.4.14 (Template.Parameter):
	// a definition outside the NBIRTH or with a template_ref, an instance
	// without is_definition or template_ref, a member or parameter the
	// definition does not have, an instance in a birth without all members
	// of its definition, or a template nested more than 32 deep.
	ErrInvalidTemplate = errors.New("sparkplugb: invalid template")
)

// maxTemplateDepth is how deep templates may be nested in a metric: a
// template is at depth 1, a template among its members at depth 2. Sparkplug
// gives no limit; a template nested deeper returns ErrInvalidTemplate.
const maxTemplateDepth = 32

// TemplateDefs are the template definitions of an edge node's NBIRTH, by the
// name of the metric that carries each one: the names that the template_ref
// of an instance refers to. A nil TemplateDefs has no definitions.
//
// Sparkplug gives the version of a template no matching rule, so instances
// are matched to definitions by template_ref alone; a version is only
// checked to be UTF-8.
type TemplateDefs map[string]*pb.Payload_Template

// NewTemplateDefs returns the template definitions of the metrics of an
// NBIRTH, and checks them and the template instances of the NBIRTH against
// Sparkplug 3.0.0. The definitions are returned even when the error is not
// nil, so that a receiver can go on checking the messages that follow: every
// definition with a metric name is, the first of a name given twice, also one
// that breaks a rule. The SparkplugNet edge node, for one, sends an empty
// template_ref with its definitions, which Sparkplug requires to be omitted:
// its instances still refer to them. The error wraps ErrInvalidTemplate or
// ErrUnknownTemplate, and is the first one found.
func NewTemplateDefs(nbirth []*pb.Payload_Metric) (TemplateDefs, error) {
	var defs TemplateDefs
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	for _, m := range nbirth {
		t := m.GetTemplateValue()
		if t == nil || !t.GetIsDefinition() {
			continue
		}
		name := m.GetName()
		if err := checkDefinition(name, t); err != nil {
			keep(err)
			if name == "" {
				continue
			}
		}
		if _, dup := defs[name]; dup {
			keep(fmt.Errorf("sparkplugb: template definition %q is in the NBIRTH twice: %w", name, ErrInvalidTemplate))
			continue
		}
		if defs == nil {
			defs = make(TemplateDefs)
		}
		defs[name] = t
	}
	c := defs.newChecker()
	for _, m := range nbirth {
		t := m.GetTemplateValue()
		if t == nil {
			continue
		}
		var err error
		if t.GetIsDefinition() {
			err = defs.checkNested([]string{metricLabel(m)}, t)
		} else {
			err = c.checkInstance([]string{metricLabel(m)}, t, true)
		}
		if err != nil {
			keep(err)
		}
	}
	return defs, first
}

// Check checks the template instances in the metrics of a DBIRTH (birth
// true), NDATA or DDATA (birth false) against the definitions of the NBIRTH:
// a definition is not allowed outside the NBIRTH, every instance must refer
// to one of defs, and an instance in a birth must have all members of its
// definition, one in data a subset. The error wraps ErrInvalidTemplate or
// ErrUnknownTemplate, and is the first one found.
func (d TemplateDefs) Check(metrics []*pb.Payload_Metric, birth bool) error {
	c := d.newChecker()
	for _, m := range metrics {
		t := m.GetTemplateValue()
		if t == nil {
			continue
		}
		name := metricLabel(m)
		if t.GetIsDefinition() {
			return fmt.Errorf("sparkplugb: template definition %q outside the NBIRTH: %w", name, ErrInvalidTemplate)
		}
		if err := c.checkInstance([]string{name}, t, birth); err != nil {
			return err
		}
	}
	return nil
}

// checkDefinition checks a definition by itself (section 6.4.13): it has no
// template_ref, and its version and parameters are well formed.
func checkDefinition(name string, t *pb.Payload_Template) error {
	if name == "" {
		return fmt.Errorf("sparkplugb: template definition without a metric name: %w", ErrInvalidTemplate)
	}
	if t.TemplateRef != nil {
		return fmt.Errorf("sparkplugb: template definition %q has a template_ref: %w", name, ErrInvalidTemplate)
	}
	if err := checkVersion([]string{name}, t); err != nil {
		return err
	}
	for _, p := range t.GetParameters() {
		if p.Name == nil {
			return fmt.Errorf("sparkplugb: parameter of template definition %q without a name: %w", name, ErrInvalidTemplate)
		}
		if p.Type == nil {
			return fmt.Errorf("sparkplugb: parameter %q of template definition %q without a type: %w", p.GetName(), name, ErrInvalidTemplate)
		}
		if err := checkParameter([]string{name}, p); err != nil {
			return err
		}
	}
	return nil
}

// label is the name of a template in an error: the names of the metrics
// from the outermost one to it, joined by "/". It is built only for an
// error: built at every level, the labels of a deep nesting would add up to
// the square of its depth.
func label(path []string) string {
	return strings.Join(path, "/")
}

// deeper returns path with the member name added, or ErrInvalidTemplate when
// that template would be nested deeper than maxTemplateDepth.
func deeper(path []string, name string) ([]string, error) {
	if len(path) >= maxTemplateDepth {
		return nil, fmt.Errorf("sparkplugb: template %q nests templates deeper than %d: %w", label(path), maxTemplateDepth, ErrInvalidTemplate)
	}
	return append(path, name), nil
}

// checkNested checks the templates among the members of a definition: an
// instance there must refer to a definition of the NBIRTH. Its members are
// not checked against that definition: Sparkplug does not say how complete
// a nested instance of a definition is.
func (d TemplateDefs) checkNested(path []string, def *pb.Payload_Template) error {
	for _, member := range def.GetMetrics() {
		t := member.GetTemplateValue()
		if t == nil {
			continue
		}
		mp, err := deeper(path, member.GetName())
		if err != nil {
			return err
		}
		if !t.GetIsDefinition() {
			if _, err := d.definition(mp, t); err != nil {
				return err
			}
		}
		if err := d.checkNested(mp, t); err != nil {
			return err
		}
	}
	return nil
}

// definition checks the is_definition and template_ref of an instance and
// returns the definition it refers to.
func (d TemplateDefs) definition(path []string, t *pb.Payload_Template) (*pb.Payload_Template, error) {
	if t.IsDefinition == nil {
		return nil, fmt.Errorf("sparkplugb: template instance %q without is_definition: %w", label(path), ErrInvalidTemplate)
	}
	if t.TemplateRef == nil || t.GetTemplateRef() == "" {
		return nil, fmt.Errorf("sparkplugb: template instance %q without a template_ref: %w", label(path), ErrInvalidTemplate)
	}
	ref := t.GetTemplateRef()
	if !utf8.ValidString(ref) {
		return nil, fmt.Errorf("sparkplugb: template_ref of template instance %q is not UTF-8: %w", label(path), ErrInvalidTemplate)
	}
	def, ok := d[ref]
	if !ok {
		return nil, fmt.Errorf("sparkplugb: template instance %q refers to %q, which is not a template definition of the NBIRTH: %w",
			label(path), ref, ErrUnknownTemplate)
	}
	return def, nil
}

// checker checks instances against the definitions of one NBIRTH. It keeps
// the member and parameter names of each definition it has met, so that the
// instances of one definition share them instead of each building its own.
type checker struct {
	defs  TemplateDefs
	names map[*pb.Payload_Template]*defNames
}

// defNames are the member and parameter names of a definition.
type defNames struct {
	members map[string]struct{}
	params  map[string]struct{}
}

func (d TemplateDefs) newChecker() *checker {
	return &checker{defs: d}
}

func (c *checker) namesOf(def *pb.Payload_Template) *defNames {
	if n, ok := c.names[def]; ok {
		return n
	}
	n := &defNames{
		members: make(map[string]struct{}, len(def.GetMetrics())),
		params:  make(map[string]struct{}, len(def.GetParameters())),
	}
	for _, m := range def.GetMetrics() {
		n.members[m.GetName()] = struct{}{}
	}
	for _, p := range def.GetParameters() {
		n.params[p.GetName()] = struct{}{}
	}
	if c.names == nil {
		c.names = make(map[*pb.Payload_Template]*defNames)
	}
	c.names[def] = n
	return n
}

// checkInstance checks an instance against its definition (section 6.4.13):
// only members and parameters of the definition, and in a birth all its
// members. Nested instances among the members are checked the same way.
func (c *checker) checkInstance(path []string, t *pb.Payload_Template, birth bool) error {
	def, err := c.defs.definition(path, t)
	if err != nil {
		return err
	}
	if err := checkVersion(path, t); err != nil {
		return err
	}
	names := c.namesOf(def)
	seen := make(map[string]struct{}, len(t.GetMetrics()))
	for _, member := range t.GetMetrics() {
		mn := member.GetName()
		if mn == "" {
			// A member by alias only: the payload package has no alias map.
			continue
		}
		if _, ok := names.members[mn]; !ok {
			return fmt.Errorf("sparkplugb: template instance %q has member %q, which definition %q does not have: %w",
				label(path), mn, t.GetTemplateRef(), ErrInvalidTemplate)
		}
		seen[mn] = struct{}{}
		if nt := member.GetTemplateValue(); nt != nil {
			mp, err := deeper(path, mn)
			if err != nil {
				return err
			}
			if nt.GetIsDefinition() {
				return fmt.Errorf("sparkplugb: template definition %q inside a template instance: %w", label(mp), ErrInvalidTemplate)
			}
			if err := c.checkInstance(mp, nt, birth); err != nil {
				return err
			}
		}
	}
	if birth {
		for _, m := range def.GetMetrics() {
			if _, ok := seen[m.GetName()]; !ok {
				return fmt.Errorf("sparkplugb: template instance %q in a birth lacks member %q of definition %q: %w",
					label(path), m.GetName(), t.GetTemplateRef(), ErrInvalidTemplate)
			}
		}
	}
	for _, p := range t.GetParameters() {
		if _, ok := names.params[p.GetName()]; !ok {
			return fmt.Errorf("sparkplugb: template instance %q has parameter %q, which definition %q does not have: %w",
				label(path), p.GetName(), t.GetTemplateRef(), ErrInvalidTemplate)
		}
		if err := checkParameter(path, p); err != nil {
			return err
		}
	}
	return nil
}

func checkVersion(path []string, t *pb.Payload_Template) error {
	if t.Version != nil && !utf8.ValidString(t.GetVersion()) {
		return fmt.Errorf("sparkplugb: version of template %q is not UTF-8: %w", label(path), ErrInvalidTemplate)
	}
	return nil
}

// checkParameter checks the name and type of a parameter (section 6.4.14):
// a UTF-8 name, and a type that is one of the basic datatypes.
func checkParameter(path []string, p *pb.Payload_Template_Parameter) error {
	if !utf8.ValidString(p.GetName()) {
		return fmt.Errorf("sparkplugb: parameter name of template %q is not UTF-8: %w", label(path), ErrInvalidTemplate)
	}
	if p.Type != nil {
		if dt := pb.DataType(p.GetType()); dt < pb.DataType_Int8 || dt > pb.DataType_Text {
			return fmt.Errorf("sparkplugb: parameter %q of template %q has type %d, not a basic datatype: %w",
				p.GetName(), label(path), p.GetType(), ErrInvalidTemplate)
		}
	}
	return nil
}

// metricLabel names a metric in an error: its name, or its alias in a data
// message that leaves the name out.
func metricLabel(m *pb.Payload_Metric) string {
	if m.Name == nil && m.Alias != nil {
		return fmt.Sprintf("alias %d", m.GetAlias())
	}
	return m.GetName()
}

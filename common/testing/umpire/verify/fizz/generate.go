package fizz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"go.temporal.io/server/common/testing/umpire/verify"
)

const unsupportedProgressReason = "FizzBee semantic generation does not support temporal progress properties"

type Diagnostic struct {
	Construct string            `json:"construct"`
	Reason    string            `json:"reason"`
	Source    verify.Provenance `json:"source,omitempty"`
}

func Generate(model verify.Model) (map[string][]byte, []Diagnostic, error) {
	if err := verify.Validate(model); err != nil {
		return nil, nil, fmt.Errorf("generate FizzBee: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return nil, nil, fmt.Errorf("generate FizzBee: %w", err)
	}
	generator := generator{model: model}
	if err := generator.validateIdentifiers(); err != nil {
		return nil, nil, err
	}
	source, diagnostics := generator.source()
	return map[string][]byte{"Umpire.fizz": []byte(source)}, diagnostics, nil
}

func RenderConfig(bounds verify.Bounds) ([]byte, error) {
	if bounds.MaxDepth == 0 {
		return nil, errors.New("render FizzBee config: positive max depth is required")
	}
	return []byte(fmt.Sprintf("options:\n  max_actions: %d\n  max_concurrent_actions: 1\n  crash_on_yield: false\ndeadlock_detection: false\nliveness: \"false\"\n", bounds.MaxDepth)), nil
}

func ActionIdentifier(name string) string {
	return "Action_" + identifier(name)
}

func PropertyIdentifier(name string) string {
	return "Property_" + identifier(name)
}

func TraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	if err := verify.Validate(model); err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate FizzBee trace vocabulary: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate FizzBee trace vocabulary: %w", err)
	}
	g := generator{model: model}
	if err := g.validateIdentifiers(); err != nil {
		return verify.TraceVocabulary{}, err
	}
	vocabulary := verify.TraceVocabulary{
		Actions:      make(map[string]string, len(model.Actions)),
		Bindings:     make(map[string]map[string]string, len(model.Actions)),
		Properties:   make(map[string][]string, len(model.Properties)+len(model.Relations)*3),
		EntityExists: make(map[string]string, len(model.Entities)),
		EntityStates: make(map[string]string, len(model.Entities)),
		Relations:    make(map[string]string, len(model.Relations)),
		Identities:   map[string]string{},
		States:       map[string]string{},
	}
	for _, entity := range model.Entities {
		vocabulary.EntityExists[existsName(entity.Name)] = entity.Name
		vocabulary.EntityStates[stateName(entity.Name)] = entity.Name
		for _, id := range entity.IDs {
			vocabulary.Identities[id] = id
		}
		for _, state := range entity.States {
			vocabulary.States[state.Name] = state.Name
		}
	}
	for _, relation := range model.Relations {
		name := identifier(relation.Name)
		vocabulary.Relations[relationName(relation.Name)] = relation.Name
		vocabulary.Properties["Relation_"+name+"_endpoints"] = []string{"relation " + relation.Name + " endpoints"}
		if relation.SourceCardinality == verify.One {
			vocabulary.Properties["Cardinality_"+name+"_source"] = []string{"relation " + relation.Name + " source cardinality"}
		}
		if relation.TargetCardinality == verify.One {
			vocabulary.Properties["Cardinality_"+name+"_target"] = []string{"relation " + relation.Name + " target cardinality"}
		}
	}
	for _, action := range model.Actions {
		name := ActionIdentifier(action.Name)
		vocabulary.Actions[name] = action.Name
		bindings := make(map[string]string, len(action.Parameters))
		for _, parameter := range action.Parameters {
			bindings[identifier(parameter.Name)] = parameter.Name
		}
		vocabulary.Bindings[name] = bindings
	}
	for _, property := range model.Properties {
		vocabulary.Properties[PropertyIdentifier(property.Name)] = []string{property.Name}
	}
	return vocabulary, nil
}

type generator struct {
	model verify.Model
}

func (g generator) source() (string, []Diagnostic) {
	var out bytes.Buffer
	out.WriteString("# Generated from the Umpire verification snapshot. Do not edit.\n\n")
	for _, entity := range g.model.Entities {
		fmt.Fprintf(&out, "%s = %s\n", idsName(entity.Name), stringList(entity.IDs))
	}
	out.WriteString("\n")
	g.writeInit(&out)
	for _, action := range g.model.Actions {
		g.writeAction(&out, action)
	}
	fmt.Fprintf(&out, "# CanStep = %s\n\n", g.canStepExpr())
	for _, relation := range g.model.Relations {
		g.writeRelationAssertions(&out, relation)
	}
	var diagnostics []Diagnostic
	for _, property := range g.model.Properties {
		if property.Kind == verify.ProgressProperty {
			diagnostics = append(diagnostics, Diagnostic{Construct: "property " + property.Name, Reason: unsupportedProgressReason, Source: property.Source})
			fmt.Fprintf(&out, "# unsupported property %s: %s\n\n", property.Name, unsupportedProgressReason)
			continue
		}
		fmt.Fprintf(&out, "always assertion %s:\n", PropertyIdentifier(property.Name))
		expression := g.expr(property.Expr, map[string]string{})
		if property.Kind == verify.QuiescentProperty {
			expression = g.canStepExpr() + " or (" + expression + ")"
		}
		fmt.Fprintf(&out, "    return %s\n\n", expression)
	}
	return out.String(), diagnostics
}

func (g generator) writeInit(out *bytes.Buffer) {
	out.WriteString("action Init:\n")
	for _, entity := range g.model.Entities {
		fmt.Fprintf(out, "    %s = set(%s)\n", existsName(entity.Name), stringList(entity.InitiallyExists))
		values := make([]string, len(entity.IDs))
		for index, id := range entity.IDs {
			values[index] = strconv.Quote(id) + ": " + strconv.Quote(entity.Initial)
		}
		fmt.Fprintf(out, "    %s = {%s}\n", stateName(entity.Name), strings.Join(values, ", "))
	}
	for _, relation := range g.model.Relations {
		fmt.Fprintf(out, "    %s = set()\n", relationName(relation.Name))
	}
	if len(g.model.Entities) == 0 && len(g.model.Relations) == 0 {
		out.WriteString("    pass\n")
	}
	out.WriteString("\n")
}

func (g generator) writeAction(out *bytes.Buffer, action verify.Action) {
	fmt.Fprintf(out, "atomic action %s:\n", ActionIdentifier(action.Name))
	bindings := make(map[string]string, len(action.Parameters))
	emptyDomain := false
	for _, parameter := range action.Parameters {
		name := identifier(parameter.Name)
		bindings[parameter.Name] = name
		entity := g.entity(parameter.Type)
		if len(entity.IDs) == 0 {
			emptyDomain = true
			fmt.Fprintf(out, "    %s = \"\"\n", name)
		} else {
			fmt.Fprintf(out, "    %s = oneof %s\n", name, idsName(parameter.Type))
		}
	}
	if emptyDomain {
		out.WriteString("    require False\n")
	}
	for _, clause := range g.enabledClauses(action, bindings) {
		fmt.Fprintf(out, "    require %s\n", clause)
	}
	branches := action.Branches
	if len(branches) == 0 {
		g.writeTransition(out, action.Effects, bindings, "    ")
	} else {
		indices := make([]string, len(branches))
		for index := range branches {
			indices[index] = strconv.Itoa(index)
		}
		fmt.Fprintf(out, "    branch = oneof [%s]\n", strings.Join(indices, ", "))
		for index, branch := range branches {
			keyword := "if"
			if index != 0 {
				keyword = "elif"
			}
			fmt.Fprintf(out, "    %s branch == %d:\n", keyword, index)
			effects := append(slices.Clone(action.Effects), branch.Effects...)
			g.writeTransition(out, effects, bindings, "        ")
		}
	}
	out.WriteString("\n")
}

func (g generator) enabledClauses(action verify.Action, bindings map[string]string) []string {
	clauses := make([]string, 0, len(action.Parameters)*2+1)
	for _, parameter := range action.Parameters {
		membership := " in "
		if parameter.Binding == verify.FreshBinding {
			membership = " not in "
		}
		clauses = append(clauses, bindings[parameter.Name]+membership+existsName(parameter.Type))
	}
	for _, pair := range verify.DistinctFreshParameterPairs(action.Parameters) {
		clauses = append(clauses, bindings[pair[0].Name]+" != "+bindings[pair[1].Name])
	}
	if action.Guard.Op != "" && action.Guard.Op != verify.TrueExpr {
		clauses = append(clauses, g.expr(action.Guard, bindings))
	}
	return clauses
}

func (g generator) writeTransition(out *bytes.Buffer, effects []verify.Effect, bindings map[string]string, indent string) {
	variables := g.variables()
	for _, variable := range variables {
		constructor := "set"
		if strings.HasPrefix(variable, "state_") {
			constructor = "dict"
		}
		fmt.Fprintf(out, "%snext_%s = %s(%s)\n", indent, variable, constructor, variable)
	}
	for _, effect := range effects {
		switch effect.Kind {
		case verify.CreateEffect:
			fmt.Fprintf(out, "%snext_%s.add(%s)\n", indent, existsName(effect.Entity), bindings[effect.Ref])
			fmt.Fprintf(out, "%snext_%s[%s] = %s\n", indent, stateName(effect.Entity), bindings[effect.Ref], strconv.Quote(effect.State))
		case verify.SetStateEffect:
			fmt.Fprintf(out, "%snext_%s[%s] = %s\n", indent, stateName(effect.Entity), bindings[effect.Ref], strconv.Quote(effect.State))
		case verify.AddRelationEffect:
			fmt.Fprintf(out, "%snext_%s.add((%s, %s))\n", indent, relationName(effect.Relation), bindings[effect.Source], bindings[effect.Target])
		case verify.RemoveRelationEffect:
			fmt.Fprintf(out, "%snext_%s.discard((%s, %s))\n", indent, relationName(effect.Relation), bindings[effect.Source], bindings[effect.Target])
		default:
			panic(fmt.Sprintf("validated model has unknown effect kind %q", effect.Kind))
		}
	}
	for _, variable := range variables {
		fmt.Fprintf(out, "%s%s = next_%s\n", indent, variable, variable)
	}
	if len(variables) == 0 {
		out.WriteString(indent + "pass\n")
	}
}

func (g generator) canStepExpr() string {
	var alternatives []string
	for _, action := range g.model.Actions {
		g.enumerateBindings(action, 0, map[string]string{}, func(bindings map[string]string) {
			clauses := g.enabledClauses(action, bindings)
			if len(clauses) == 0 {
				alternatives = append(alternatives, "True")
			} else {
				alternatives = append(alternatives, "("+strings.Join(clauses, " and ")+")")
			}
		})
	}
	if len(alternatives) == 0 {
		return "False"
	}
	return "(" + strings.Join(alternatives, " or ") + ")"
}

func (g generator) enumerateBindings(action verify.Action, index int, bindings map[string]string, yield func(map[string]string)) {
	if index == len(action.Parameters) {
		yield(bindings)
		return
	}
	parameter := action.Parameters[index]
	for _, id := range g.entity(parameter.Type).IDs {
		next := cloneBindings(bindings)
		next[parameter.Name] = strconv.Quote(id)
		g.enumerateBindings(action, index+1, next, yield)
	}
}

func (g generator) writeRelationAssertions(out *bytes.Buffer, relation verify.Relation) {
	name := identifier(relation.Name)
	sources := g.entity(relation.Source).IDs
	targets := g.entity(relation.Target).IDs
	var endpoints []string
	for _, source := range sources {
		for _, target := range targets {
			tuple := "(" + strconv.Quote(source) + ", " + strconv.Quote(target) + ")"
			endpoints = append(endpoints, "("+tuple+" not in "+relationName(relation.Name)+" or ("+strconv.Quote(source)+" in "+existsName(relation.Source)+" and "+strconv.Quote(target)+" in "+existsName(relation.Target)+"))")
		}
	}
	g.writeAssertion(out, "Relation_"+name+"_endpoints", conjunction(endpoints))
	if relation.SourceCardinality == verify.One {
		var clauses []string
		for _, source := range sources {
			for left := range targets {
				for right := left + 1; right < len(targets); right++ {
					clauses = append(clauses, "not (("+strconv.Quote(source)+", "+strconv.Quote(targets[left])+") in "+relationName(relation.Name)+" and ("+strconv.Quote(source)+", "+strconv.Quote(targets[right])+") in "+relationName(relation.Name)+")")
				}
			}
		}
		g.writeAssertion(out, "Cardinality_"+name+"_source", conjunction(clauses))
	}
	if relation.TargetCardinality == verify.One {
		var clauses []string
		for _, target := range targets {
			for left := range sources {
				for right := left + 1; right < len(sources); right++ {
					clauses = append(clauses, "not (("+strconv.Quote(sources[left])+", "+strconv.Quote(target)+") in "+relationName(relation.Name)+" and ("+strconv.Quote(sources[right])+", "+strconv.Quote(target)+") in "+relationName(relation.Name)+")")
				}
			}
		}
		g.writeAssertion(out, "Cardinality_"+name+"_target", conjunction(clauses))
	}
}

func (g generator) writeAssertion(out *bytes.Buffer, name, expression string) {
	fmt.Fprintf(out, "always assertion %s:\n    return %s\n\n", name, expression)
}

func (g generator) expr(expression verify.Expr, bindings map[string]string) string {
	switch expression.Op {
	case "", verify.TrueExpr:
		return "True"
	case verify.FalseExpr:
		return "False"
	case verify.NotExpr:
		return "not (" + g.expr(expression.Args[0], bindings) + ")"
	case verify.AndExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " and ") + ")"
	case verify.OrExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " or ") + ")"
	case verify.ImpliesExpr:
		return "(not (" + g.expr(expression.Args[0], bindings) + ") or (" + g.expr(expression.Args[1], bindings) + "))"
	case verify.EntityExistsExpr:
		return bindings[expression.Ref] + " in " + existsName(expression.Entity)
	case verify.StateIsExpr:
		return stateName(expression.Entity) + "[" + bindings[expression.Ref] + "] == " + strconv.Quote(expression.State)
	case verify.RelationHasExpr:
		return "(" + bindings[expression.Source] + ", " + bindings[expression.Target] + ") in " + relationName(expression.Relation)
	case verify.ForAllExpr, verify.ExistsExpr:
		var expanded []string
		for _, value := range g.entity(expression.Entity).IDs {
			inner := cloneBindings(bindings)
			inner[expression.Var] = strconv.Quote(value)
			body := g.expr(expression.Args[0], inner)
			exists := strconv.Quote(value) + " in " + existsName(expression.Entity)
			if expression.Op == verify.ForAllExpr {
				expanded = append(expanded, "(not ("+exists+") or ("+body+"))")
			} else {
				expanded = append(expanded, "("+exists+" and ("+body+"))")
			}
		}
		if expression.Op == verify.ForAllExpr {
			return conjunction(expanded)
		}
		return disjunction(expanded)
	default:
		panic(fmt.Sprintf("validated model has unknown expression operator %q", expression.Op))
	}
}

func (g generator) joinExpr(expressions []verify.Expr, bindings map[string]string, separator string) string {
	result := make([]string, len(expressions))
	for index, expression := range expressions {
		result[index] = g.expr(expression, bindings)
	}
	return strings.Join(result, separator)
}

func (g generator) variables() []string {
	result := make([]string, 0, len(g.model.Entities)*2+len(g.model.Relations))
	for _, entity := range g.model.Entities {
		result = append(result, existsName(entity.Name), stateName(entity.Name))
	}
	for _, relation := range g.model.Relations {
		result = append(result, relationName(relation.Name))
	}
	return result
}

func (g generator) entity(name string) verify.EntityType {
	for _, entity := range g.model.Entities {
		if entity.Name == name {
			return entity
		}
	}
	panic("validated model references unknown entity " + name)
}

func (g generator) validateIdentifiers() error {
	seen := map[string]string{"CanStep": "generated CanStep function"}
	check := func(source, generated string) error {
		if previous, found := seen[generated]; found && previous != source {
			return fmt.Errorf("generate FizzBee: %q and %q normalize to identifier %q", previous, source, generated)
		}
		seen[generated] = source
		return nil
	}
	for _, entity := range g.model.Entities {
		for _, generated := range []string{idsName(entity.Name), existsName(entity.Name), stateName(entity.Name)} {
			if err := check("entity "+entity.Name, generated); err != nil {
				return err
			}
		}
	}
	for _, relation := range g.model.Relations {
		name := identifier(relation.Name)
		for _, generated := range []string{relationName(relation.Name), "Relation_" + name + "_endpoints", "Cardinality_" + name + "_source", "Cardinality_" + name + "_target"} {
			if err := check("relation "+relation.Name, generated); err != nil {
				return err
			}
		}
	}
	for _, action := range g.model.Actions {
		if err := check("action "+action.Name, ActionIdentifier(action.Name)); err != nil {
			return err
		}
		parameters := map[string]string{"branch": "generated branch choice"}
		for _, parameter := range action.Parameters {
			generated := identifier(parameter.Name)
			if previous, found := parameters[generated]; found {
				return fmt.Errorf("generate FizzBee: action %q parameters %q and %q normalize to identifier %q", action.Name, previous, parameter.Name, generated)
			}
			if strings.HasPrefix(generated, "next_") {
				return fmt.Errorf("generate FizzBee: action %q parameter %q uses reserved generated prefix next_", action.Name, parameter.Name)
			}
			parameters[generated] = parameter.Name
		}
	}
	for _, property := range g.model.Properties {
		if err := check("property "+property.Name, PropertyIdentifier(property.Name)); err != nil {
			return err
		}
	}
	return nil
}

func canonicalModel(model verify.Model) (verify.Model, error) {
	encoded, err := verify.MarshalModel(model)
	if err != nil {
		return verify.Model{}, err
	}
	var result verify.Model
	if err := json.Unmarshal(encoded, &result); err != nil {
		return verify.Model{}, err
	}
	return result, nil
}

func identifier(value string) string {
	var result strings.Builder
	for index, character := range value {
		if unicode.IsLetter(character) || character == '_' || index > 0 && unicode.IsDigit(character) {
			result.WriteRune(character)
		} else if index == 0 && unicode.IsDigit(character) {
			result.WriteString("n_")
			result.WriteRune(character)
		} else {
			result.WriteRune('_')
		}
	}
	if result.Len() == 0 {
		return "unnamed"
	}
	return result.String()
}

func idsName(name string) string      { return "IDs_" + identifier(name) }
func existsName(name string) string   { return "exists_" + identifier(name) }
func stateName(name string) string    { return "state_" + identifier(name) }
func relationName(name string) string { return "relation_" + identifier(name) }

func stringList(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = strconv.Quote(value)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func conjunction(expressions []string) string {
	if len(expressions) == 0 {
		return "True"
	}
	return "(" + strings.Join(expressions, " and ") + ")"
}

func disjunction(expressions []string) string {
	if len(expressions) == 0 {
		return "False"
	}
	return "(" + strings.Join(expressions, " or ") + ")"
}

func cloneBindings(bindings map[string]string) map[string]string {
	result := make(map[string]string, len(bindings))
	for name, value := range bindings {
		result[name] = value
	}
	return result
}

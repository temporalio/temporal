package p

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"go.temporal.io/server/common/testing/umpire/verify"
)

func Generate(model verify.Model) (map[string][]byte, error) {
	if err := verify.Validate(model); err != nil {
		return nil, fmt.Errorf("generate P: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return nil, fmt.Errorf("generate P: %w", err)
	}
	g := generator{model: model}
	if err := g.validateIdentifiers(); err != nil {
		return nil, err
	}
	return map[string][]byte{
		"Umpire.p":     []byte(g.source()),
		"Umpire.pproj": []byte(project),
	}, nil
}

const project = `<Project>
<ProjectName>Umpire</ProjectName>
<InputFiles>
  <PFile>./Umpire.p</PFile>
</InputFiles>
<OutputDir>./PGenerated/</OutputDir>
<Target>PChecker,PEx</Target>
</Project>
`

type generator struct {
	model verify.Model
}

type candidate struct {
	code     int
	action   verify.Action
	bindings map[string]string
}

func (g generator) source() string {
	var out bytes.Buffer
	out.WriteString("// Generated from the Umpire verification snapshot. Do not edit.\n\n")
	out.WriteString("event eStep;\n\n")
	for _, entity := range g.model.Entities {
		values := make([]string, len(entity.IDs))
		for index, value := range entity.IDs {
			values[index] = entityID(value)
		}
		fmt.Fprintf(&out, "enum %s { %s }\n", identifier(entity.Name), strings.Join(values, ", "))
		if len(entity.States) != 0 {
			states := make([]string, len(entity.States))
			for index, state := range entity.States {
				states[index] = stateID(entity.Name, state.Name)
			}
			fmt.Fprintf(&out, "enum %s { %s }\n", stateType(entity.Name), strings.Join(states, ", "))
		}
	}
	for _, relation := range g.model.Relations {
		fmt.Fprintf(&out, "type %s = (source: %s, target: %s);\n", relationType(relation.Name), identifier(relation.Source), identifier(relation.Target))
	}
	out.WriteString("\nmachine UmpireWorld {\n")
	out.WriteString("  var checkerStep: int;\n")
	for _, entity := range g.model.Entities {
		fmt.Fprintf(&out, "  var %s: set[%s];\n", existsName(entity.Name), identifier(entity.Name))
		if len(entity.States) != 0 {
			fmt.Fprintf(&out, "  var %s: map[%s, %s];\n", stateName(entity.Name), identifier(entity.Name), stateType(entity.Name))
		}
	}
	for _, relation := range g.model.Relations {
		fmt.Fprintf(&out, "  var %s: set[%s];\n", relationName(relation.Name), relationType(relation.Name))
	}
	out.WriteString("\n  start state Init {\n    entry {\n")
	for _, entity := range g.model.Entities {
		for _, value := range entity.IDs {
			if len(entity.States) != 0 {
				fmt.Fprintf(&out, "      %s[%s] = %s;\n", stateName(entity.Name), entityID(value), stateID(entity.Name, entity.Initial))
			}
		}
		for _, value := range entity.InitiallyExists {
			fmt.Fprintf(&out, "      %s += (%s);\n", existsName(entity.Name), entityID(value))
		}
	}
	out.WriteString("      CheckSafety();\n      send this, eStep;\n    }\n    on eStep do Step;\n  }\n\n")
	candidates := g.candidates()
	g.writeStep(&out, candidates)
	for _, current := range candidates {
		g.writeApply(&out, current)
	}
	g.writeSafety(&out)
	g.writeQuiescent(&out)
	out.WriteString("}\n\n")
	out.WriteString("test tcUmpire [main=UmpireWorld]: { UmpireWorld };\n")
	return out.String()
}

func (g generator) candidates() []candidate {
	var result []candidate
	for _, action := range g.model.Actions {
		bindings := map[string]string{}
		var enumerate func(int)
		enumerate = func(index int) {
			if index == len(action.Parameters) {
				cloned := make(map[string]string, len(bindings))
				for name, value := range bindings {
					cloned[name] = value
				}
				result = append(result, candidate{code: len(result), action: action, bindings: cloned})
				return
			}
			parameter := action.Parameters[index]
			for _, value := range g.entity(parameter.Type).IDs {
				bindings[parameter.Name] = value
				enumerate(index + 1)
			}
			delete(bindings, parameter.Name)
		}
		enumerate(0)
	}
	return result
}

func (g generator) writeStep(out *bytes.Buffer, candidates []candidate) {
	out.WriteString("  fun Step() {\n    var enabled: set[int];\n")
	for _, current := range candidates {
		fmt.Fprintf(out, "    if (%s) { enabled += (%d); }\n", g.enabled(current), current.code)
	}
	out.WriteString("    if (sizeof(enabled) == 0) {\n      CheckQuiescent();\n      raise halt;\n    }\n")
	for _, current := range candidates {
		fmt.Fprintf(out, "    if (%d in enabled) {\n", current.code)
		fmt.Fprintf(out, "      if (sizeof(enabled) == 1) { %s(); return; }\n", applyName(current))
		fmt.Fprintf(out, "      if ($) { %s(); return; }\n", applyName(current))
		fmt.Fprintf(out, "      enabled -= (%d);\n", current.code)
		out.WriteString("    }\n")
	}
	out.WriteString("  }\n\n")
}

func (g generator) enabled(current candidate) string {
	var clauses []string
	for _, parameter := range current.action.Parameters {
		value := entityID(current.bindings[parameter.Name])
		membership := fmt.Sprintf("%s in %s", value, existsName(parameter.Type))
		if parameter.Binding == verify.FreshBinding {
			membership = "!(" + membership + ")"
		}
		clauses = append(clauses, membership)
	}
	for _, pair := range verify.DistinctFreshParameterPairs(current.action.Parameters) {
		clauses = append(clauses, entityID(current.bindings[pair[0].Name])+" != "+entityID(current.bindings[pair[1].Name]))
	}
	if current.action.Guard.Op != "" && current.action.Guard.Op != verify.TrueExpr {
		clauses = append(clauses, g.expr(current.action.Guard, current.bindings))
	}
	if len(clauses) == 0 {
		return "true"
	}
	return strings.Join(clauses, " && ")
}

func (g generator) writeApply(out *bytes.Buffer, current candidate) {
	fmt.Fprintf(out, "  fun %s() {\n", applyName(current))
	trace := current.action.Name
	for _, parameter := range current.action.Parameters {
		trace += " " + parameter.Name + "=" + current.bindings[parameter.Name]
	}
	fmt.Fprintf(out, "    print %s;\n", strconv.Quote("UMPIRE_ACTION "+trace))
	branches := current.action.Branches
	if len(branches) == 0 {
		g.writeEffects(out, current.action.Effects, current.bindings, "    ")
	} else {
		g.writeBranch(out, current, 0, "    ")
	}
	// PEx treats any repeated complete machine state as an implicit liveness failure. The source
	// protocol permits retry cycles, so keep bounded checker steps distinct without changing guards
	// or effects in the protocol state.
	out.WriteString("    CheckSafety();\n    checkerStep = checkerStep + 1;\n    send this, eStep;\n  }\n\n")
}

func (g generator) writeBranch(out *bytes.Buffer, current candidate, index int, indent string) {
	branch := current.action.Branches[index]
	if index == len(current.action.Branches)-1 {
		g.writeEffects(out, appendEffects(current.action.Effects, branch.Effects), current.bindings, indent)
		return
	}
	out.WriteString(indent + "if ($) {\n")
	g.writeEffects(out, appendEffects(current.action.Effects, branch.Effects), current.bindings, indent+"  ")
	out.WriteString(indent + "} else {\n")
	g.writeBranch(out, current, index+1, indent+"  ")
	out.WriteString(indent + "}\n")
}

func (g generator) writeEffects(out *bytes.Buffer, effects []verify.Effect, bindings map[string]string, indent string) {
	for _, effect := range effects {
		switch effect.Kind {
		case verify.CreateEffect:
			value := entityID(bindings[effect.Ref])
			fmt.Fprintf(out, "%s%s += (%s);\n", indent, existsName(effect.Entity), value)
			fmt.Fprintf(out, "%s%s[%s] = %s;\n", indent, stateName(effect.Entity), value, stateID(effect.Entity, effect.State))
		case verify.SetStateEffect:
			fmt.Fprintf(out, "%s%s[%s] = %s;\n", indent, stateName(effect.Entity), entityID(bindings[effect.Ref]), stateID(effect.Entity, effect.State))
		case verify.AddRelationEffect:
			fmt.Fprintf(out, "%s%s += (%s);\n", indent, relationName(effect.Relation), tuple(bindings[effect.Source], bindings[effect.Target]))
		case verify.RemoveRelationEffect:
			fmt.Fprintf(out, "%s%s -= (%s);\n", indent, relationName(effect.Relation), tuple(bindings[effect.Source], bindings[effect.Target]))
		default:
			panic(fmt.Sprintf("validated model has unknown effect kind %q", effect.Kind))
		}
	}
}

func (g generator) writeSafety(out *bytes.Buffer) {
	out.WriteString("  fun CheckSafety() {\n")
	for _, relation := range g.model.Relations {
		sources := g.entity(relation.Source).IDs
		targets := g.entity(relation.Target).IDs
		for _, source := range sources {
			for _, target := range targets {
				fmt.Fprintf(out, "    assert !(%s) || (%s in %s && %s in %s), %s;\n",
					g.relationContains(relation.Name, source, target),
					entityID(source), existsName(relation.Source), entityID(target), existsName(relation.Target),
					strconv.Quote("relation "+relation.Name+" has an absent endpoint"),
				)
			}
		}
		if relation.SourceCardinality == verify.One {
			for _, source := range sources {
				for left := range targets {
					for right := left + 1; right < len(targets); right++ {
						fmt.Fprintf(out, "    assert !(%s && %s), %s;\n", g.relationContains(relation.Name, source, targets[left]), g.relationContains(relation.Name, source, targets[right]), strconv.Quote("relation "+relation.Name+" exceeds source cardinality"))
					}
				}
			}
		}
		if relation.TargetCardinality == verify.One {
			for _, target := range targets {
				for left := range sources {
					for right := left + 1; right < len(sources); right++ {
						fmt.Fprintf(out, "    assert !(%s && %s), %s;\n", g.relationContains(relation.Name, sources[left], target), g.relationContains(relation.Name, sources[right], target), strconv.Quote("relation "+relation.Name+" exceeds target cardinality"))
					}
				}
			}
		}
	}
	for _, property := range g.model.Properties {
		if property.Kind == verify.SafetyProperty {
			fmt.Fprintf(out, "    assert %s, %s;\n", g.expr(property.Expr, map[string]string{}), strconv.Quote("property "+property.Name+" failed"))
		}
	}
	out.WriteString("  }\n\n")
}

func (g generator) writeQuiescent(out *bytes.Buffer) {
	out.WriteString("  fun CheckQuiescent() {\n")
	for _, property := range g.model.Properties {
		if property.Kind == verify.QuiescentProperty {
			fmt.Fprintf(out, "    assert %s, %s;\n", g.expr(property.Expr, map[string]string{}), strconv.Quote("quiescent property "+property.Name+" failed"))
		}
	}
	out.WriteString("  }\n\n")
}

func (g generator) expr(expression verify.Expr, bindings map[string]string) string {
	switch expression.Op {
	case "", verify.TrueExpr:
		return "true"
	case verify.NotExpr:
		return "!(" + g.expr(expression.Args[0], bindings) + ")"
	case verify.AndExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " && ") + ")"
	case verify.OrExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " || ") + ")"
	case verify.ImpliesExpr:
		return "(!(" + g.expr(expression.Args[0], bindings) + ") || (" + g.expr(expression.Args[1], bindings) + "))"
	case verify.EntityExistsExpr:
		return entityID(bindings[expression.Ref]) + " in " + existsName(expression.Entity)
	case verify.StateIsExpr:
		return stateName(expression.Entity) + "[" + entityID(bindings[expression.Ref]) + "] == " + stateID(expression.Entity, expression.State)
	case verify.RelationHasExpr:
		return g.relationContains(expression.Relation, bindings[expression.Source], bindings[expression.Target])
	case verify.ForAllExpr, verify.ExistsExpr:
		var expanded []string
		for _, value := range g.entity(expression.Entity).IDs {
			inner := cloneBindings(bindings)
			inner[expression.Var] = value
			body := g.expr(expression.Args[0], inner)
			exists := entityID(value) + " in " + existsName(expression.Entity)
			if expression.Op == verify.ForAllExpr {
				expanded = append(expanded, "(!("+exists+") || ("+body+"))")
			} else {
				expanded = append(expanded, "("+exists+" && ("+body+"))")
			}
		}
		if len(expanded) == 0 {
			return map[bool]string{true: "true", false: "false"}[expression.Op == verify.ForAllExpr]
		}
		separator := " && "
		if expression.Op == verify.ExistsExpr {
			separator = " || "
		}
		return "(" + strings.Join(expanded, separator) + ")"
	default:
		return "false"
	}
}

func (g generator) joinExpr(expressions []verify.Expr, bindings map[string]string, separator string) string {
	result := make([]string, len(expressions))
	for index, expression := range expressions {
		result[index] = g.expr(expression, bindings)
	}
	return strings.Join(result, separator)
}

func (g generator) relationContains(relation, source, target string) string {
	return tuple(source, target) + " in " + relationName(relation)
}

func (g generator) entity(name string) verify.EntityType {
	for _, entity := range g.model.Entities {
		if entity.Name == name {
			return entity
		}
	}
	return verify.EntityType{}
}

func (g generator) validateIdentifiers() error {
	seen := map[string]string{}
	check := func(source, target string) error {
		if previous, found := seen[target]; found && previous != source {
			return fmt.Errorf("generate P: %q and %q normalize to identifier %q", previous, source, target)
		}
		seen[target] = source
		return nil
	}
	for _, entity := range g.model.Entities {
		if err := check(entity.Name, identifier(entity.Name)); err != nil {
			return err
		}
		for _, value := range entity.IDs {
			if err := check(value, entityID(value)); err != nil {
				return err
			}
		}
		for _, state := range entity.States {
			if err := check(entity.Name+"."+state.Name, stateID(entity.Name, state.Name)); err != nil {
				return err
			}
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
	err = json.Unmarshal(encoded, &result)
	return result, err
}

func appendEffects(common, branch []verify.Effect) []verify.Effect {
	result := make([]verify.Effect, 0, len(common)+len(branch))
	result = append(result, common...)
	return append(result, branch...)
}

func cloneBindings(source map[string]string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for name, value := range source {
		result[name] = value
	}
	return result
}

func applyName(current candidate) string {
	result := "Apply_" + identifier(current.action.Name)
	for _, parameter := range current.action.Parameters {
		result += "_" + entityID(current.bindings[parameter.Name])
	}
	return result
}

func tuple(source, target string) string {
	return "(source = " + entityID(source) + ", target = " + entityID(target) + ")"
}

func identifier(value string) string {
	var result []rune
	for _, current := range value {
		if unicode.IsLetter(current) || unicode.IsDigit(current) || current == '_' {
			result = append(result, current)
		} else if len(result) == 0 || result[len(result)-1] != '_' {
			result = append(result, '_')
		}
	}
	for len(result) != 0 && result[len(result)-1] == '_' {
		result = result[:len(result)-1]
	}
	if len(result) == 0 || !unicode.IsLetter(result[0]) {
		result = append([]rune{'x', '_'}, result...)
	}
	return string(result)
}

func entityID(value string) string        { return identifier(value) }
func stateType(entity string) string      { return identifier(entity) + "_state" }
func stateID(entity, state string) string { return stateType(entity) + "_" + identifier(state) }
func existsName(entity string) string     { return "exists_" + identifier(entity) }
func stateName(entity string) string      { return "state_" + identifier(entity) }
func relationName(relation string) string { return "relation_" + identifier(relation) }
func relationType(relation string) string { return relationName(relation) + "_tuple" }

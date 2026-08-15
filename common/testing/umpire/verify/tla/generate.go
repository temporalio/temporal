package tla

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"go.temporal.io/server/common/testing/umpire/verify"
)

func Generate(model verify.Model) (map[string][]byte, error) {
	if err := verify.Validate(model); err != nil {
		return nil, fmt.Errorf("generate TLA+: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return nil, fmt.Errorf("generate TLA+: %w", err)
	}
	generator := generator{model: model}
	module, err := generator.module()
	if err != nil {
		return nil, err
	}
	config := generator.config()
	return map[string][]byte{
		"Umpire.tla":         []byte(module),
		"Umpire-smoke.cfg":   []byte(config),
		"Umpire-nightly.cfg": []byte(config),
	}, nil
}

func TraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	if err := verify.Validate(model); err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate TLA+ trace vocabulary: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate TLA+ trace vocabulary: %w", err)
	}
	g := generator{model: model}
	if err := g.validateIdentifiers(); err != nil {
		return verify.TraceVocabulary{}, err
	}
	vocabulary := verify.TraceVocabulary{
		Actions:      make(map[string]string, len(model.Actions)),
		Bindings:     make(map[string]map[string]string, len(model.Actions)),
		Properties:   make(map[string][]string, len(model.Properties)+len(model.Relations)),
		EntityExists: make(map[string]string, len(model.Entities)),
		EntityStates: make(map[string]string, len(model.Entities)),
		Relations:    make(map[string]string, len(model.Relations)),
		Identities:   map[string]string{},
		States:       map[string]string{},
	}
	var inductiveProperties []string
	var declaredSafetyProperties []string
	var quiescentProperties []string
	for _, entity := range model.Entities {
		vocabulary.EntityExists[existsVariable(entity.Name)] = entity.Name
		vocabulary.EntityStates[stateVariable(entity.Name)] = entity.Name
	}
	for _, relation := range model.Relations {
		vocabulary.Relations[relationVariable(relation.Name)] = relation.Name
		properties := []string{"relation " + relation.Name + " endpoints"}
		if relation.SourceCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" source cardinality")
		}
		if relation.TargetCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" target cardinality")
		}
		vocabulary.Properties[cardinalityIdentifier(relation.Name)] = properties
		inductiveProperties = append(inductiveProperties, properties...)
	}
	for _, action := range model.Actions {
		actionName := actionIdentifier(action.Name)
		vocabulary.Actions[actionName] = action.Name
		bindings := make(map[string]string, len(action.Parameters))
		for _, parameter := range action.Parameters {
			bindings[identifier(parameter.Name)] = parameter.Name
		}
		vocabulary.Bindings[actionName] = bindings
	}
	for _, property := range model.Properties {
		vocabulary.Properties[propertyIdentifier(property.Name)] = []string{property.Name}
		switch property.Kind {
		case verify.SafetyProperty:
			inductiveProperties = append(inductiveProperties, property.Name)
			if !property.Strengthening {
				declaredSafetyProperties = append(declaredSafetyProperties, property.Name)
			}
		case verify.QuiescentProperty:
			quiescentProperties = append(quiescentProperties, property.Name)
		default:
		}
	}
	vocabulary.Properties["InductiveInvariant"] = slices.Clone(inductiveProperties)
	vocabulary.Properties["DeclaredSafety"] = slices.Clone(declaredSafetyProperties)
	vocabulary.Properties["Safety"] = slices.Clone(inductiveProperties)
	vocabulary.Properties["QuiescentSafety"] = slices.Clone(quiescentProperties)
	return vocabulary, nil
}

type generator struct {
	model verify.Model
}

func (g generator) module() (string, error) {
	if err := g.validateIdentifiers(); err != nil {
		return "", err
	}
	var out bytes.Buffer
	out.WriteString("---- MODULE Umpire ----\n")
	out.WriteString("EXTENDS FiniteSets, Naturals\n\n")
	out.WriteString("CONSTANTS\n")
	for index, entity := range g.model.Entities {
		out.WriteString("    \\* @type: Set(Str);\n")
		out.WriteString("    " + entityConstant(entity.Name))
		if index != len(g.model.Entities)-1 {
			out.WriteString(",")
		}
		out.WriteString("\n")
	}
	out.WriteString("\n")

	variables := g.variables()
	out.WriteString("VARIABLES\n")
	for index, variable := range variables {
		out.WriteString("    \\* @type: " + g.variableType(variable) + ";\n")
		out.WriteString("    " + variable)
		if index != len(variables)-1 {
			out.WriteString(",")
		}
		out.WriteString("\n")
	}
	out.WriteString("\n")
	out.WriteString("vars == <<")
	out.WriteString(strings.Join(variables, ", "))
	out.WriteString(">>\n\n")

	g.writeTypeOK(&out)
	g.writeCardinality(&out)
	g.writeInit(&out)
	for _, action := range g.model.Actions {
		g.writeAction(&out, action)
	}
	g.writeNext(&out)
	g.writeCanStep(&out)
	for _, property := range g.model.Properties {
		out.WriteString(propertyIdentifier(property.Name))
		out.WriteString(" ==\n    ")
		out.WriteString(g.expr(property.Expr))
		out.WriteString("\n\n")
	}
	g.writeSafety(&out)
	g.writeQuiescentSafety(&out)
	out.WriteString("Spec == Init /\\ [][Next]_vars\n\n")
	out.WriteString("====\n")
	return out.String(), nil
}

func (g generator) variableType(variable string) string {
	for _, entity := range g.model.Entities {
		if variable == existsVariable(entity.Name) {
			return "Set(Str)"
		}
		if variable == stateVariable(entity.Name) {
			return "Str -> Str"
		}
	}
	return "Set(<<Str, Str>>)"
}

func (g generator) validateIdentifiers() error {
	seen := map[string]string{}
	check := func(kind, source, identifier string) error {
		if previous, duplicate := seen[identifier]; duplicate {
			return fmt.Errorf("generate TLA+: %s %q and %s normalize to identifier %q", kind, source, previous, identifier)
		}
		seen[identifier] = kind + " " + source
		return nil
	}
	for _, entity := range g.model.Entities {
		if err := check("entity", entity.Name, identifier(entity.Name)); err != nil {
			return err
		}
	}
	for _, relation := range g.model.Relations {
		if err := check("relation", relation.Name, identifier(relation.Name)); err != nil {
			return err
		}
	}
	for _, action := range g.model.Actions {
		if err := check("action", action.Name, actionIdentifier(action.Name)); err != nil {
			return err
		}
		parameters := map[string]string{}
		for _, parameter := range action.Parameters {
			generated := identifier(parameter.Name)
			if previous, duplicate := parameters[generated]; duplicate {
				return fmt.Errorf(
					"generate TLA+: action %q parameters %q and %q normalize to parameter identifier %q",
					action.Name, previous, parameter.Name, generated,
				)
			}
			parameters[generated] = parameter.Name
		}
	}
	for _, property := range g.model.Properties {
		if err := check("property", property.Name, propertyIdentifier(property.Name)); err != nil {
			return err
		}
	}
	return nil
}

func (g generator) variables() []string {
	var result []string
	for _, entity := range g.model.Entities {
		result = append(result, existsVariable(entity.Name))
		if len(entity.States) != 0 {
			result = append(result, stateVariable(entity.Name))
		}
	}
	for _, relation := range g.model.Relations {
		result = append(result, relationVariable(relation.Name))
	}
	return result
}

func (g generator) writeTypeOK(out *bytes.Buffer) {
	out.WriteString("TypeOK ==\n")
	var clauses []string
	for _, entity := range g.model.Entities {
		clauses = append(clauses, fmt.Sprintf("%s \\in SUBSET %s", existsVariable(entity.Name), entityConstant(entity.Name)))
		if len(entity.States) != 0 {
			clauses = append(clauses, fmt.Sprintf("%s \\in [%s -> %s]", stateVariable(entity.Name), entityConstant(entity.Name), stringSet(stateNames(entity))))
		}
	}
	for _, relation := range g.model.Relations {
		clauses = append(clauses, fmt.Sprintf("%s \\in SUBSET (%s \\X %s)", relationVariable(relation.Name), entityConstant(relation.Source), entityConstant(relation.Target)))
	}
	writeClauses(out, clauses)
	out.WriteString("\n")
}

func (g generator) writeCardinality(out *bytes.Buffer) {
	for _, relation := range g.model.Relations {
		name := cardinalityIdentifier(relation.Name)
		out.WriteString(name)
		out.WriteString(" ==\n")
		clauses := []string{fmt.Sprintf("\\A tuple \\in %s: tuple[1] \\in %s /\\ tuple[2] \\in %s", relationVariable(relation.Name), existsVariable(relation.Source), existsVariable(relation.Target))}
		if relation.SourceCardinality == verify.One {
			clauses = append(clauses, fmt.Sprintf("\\A source \\in %s: Cardinality({target \\in %s: <<source, target>> \\in %s}) <= 1", entityConstant(relation.Source), entityConstant(relation.Target), relationVariable(relation.Name)))
		}
		if relation.TargetCardinality == verify.One {
			clauses = append(clauses, fmt.Sprintf("\\A target \\in %s: Cardinality({source \\in %s: <<source, target>> \\in %s}) <= 1", entityConstant(relation.Target), entityConstant(relation.Source), relationVariable(relation.Name)))
		}
		if len(clauses) == 0 {
			clauses = []string{"TRUE"}
		}
		writeClauses(out, clauses)
		out.WriteString("\n")
	}
}

func (g generator) writeInit(out *bytes.Buffer) {
	out.WriteString("Init ==\n")
	var clauses []string
	for _, entity := range g.model.Entities {
		clauses = append(clauses, fmt.Sprintf("%s = %s", existsVariable(entity.Name), stringSet(entity.InitiallyExists)))
		if len(entity.States) != 0 {
			clauses = append(clauses, fmt.Sprintf("%s = [entity \\in %s |-> %q]", stateVariable(entity.Name), entityConstant(entity.Name), entity.Initial))
		}
	}
	for _, relation := range g.model.Relations {
		clauses = append(clauses, relationVariable(relation.Name)+" = {}")
	}
	writeClauses(out, clauses)
	out.WriteString("\n")
}

func (g generator) writeAction(out *bytes.Buffer, action verify.Action) {
	name := actionIdentifier(action.Name)
	parameters := parameterNames(action.Parameters)
	out.WriteString(name)
	out.WriteString("Enabled")
	writeParameters(out, parameters)
	out.WriteString(" ==\n")
	writeClauses(out, g.enabledClauses(action))
	out.WriteString("\n")

	out.WriteString(name)
	writeParameters(out, parameters)
	out.WriteString(" ==\n")
	out.WriteString("    /\\ ")
	out.WriteString(name)
	out.WriteString("Enabled")
	writeArguments(out, parameters)
	out.WriteString("\n")
	branches := action.Branches
	if len(branches) == 0 {
		branches = []verify.Branch{{Effects: nil}}
	}
	if len(branches) == 1 {
		g.writeTransition(out, append(slices.Clone(action.Effects), branches[0].Effects...), "    ")
	} else {
		out.WriteString("    /\\ \\/ ")
		for index, branch := range branches {
			if index != 0 {
				out.WriteString("       \\/ ")
			}
			out.WriteString("/\\ TRUE\n")
			g.writeTransition(out, append(slices.Clone(action.Effects), branch.Effects...), "          ")
		}
	}
	out.WriteString("\n")
}

func (g generator) enabledClauses(action verify.Action) []string {
	clauses := make([]string, 0, len(action.Parameters)*2+1)
	for _, parameter := range action.Parameters {
		clauses = append(clauses, fmt.Sprintf("%s \\in %s", identifier(parameter.Name), entityConstant(parameter.Type)))
		membership := "\\in"
		if parameter.Binding == verify.FreshBinding {
			membership = "\\notin"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s %s", identifier(parameter.Name), membership, existsVariable(parameter.Type)))
	}
	for _, pair := range verify.DistinctFreshParameterPairs(action.Parameters) {
		clauses = append(clauses, fmt.Sprintf("%s /= %s", identifier(pair[0].Name), identifier(pair[1].Name)))
	}
	if action.Guard.Op != "" && action.Guard.Op != verify.TrueExpr {
		clauses = append(clauses, g.expr(action.Guard))
	}
	if len(clauses) == 0 {
		return []string{"TRUE"}
	}
	return clauses
}

func (g generator) writeTransition(out *bytes.Buffer, effects []verify.Effect, indent string) {
	modified := map[string]struct{}{}
	for _, entity := range g.model.Entities {
		var creates []string
		var stateUpdates []verify.Effect
		for _, effect := range effects {
			if effect.Entity != entity.Name {
				continue
			}
			if effect.Kind == verify.CreateEffect {
				creates = append(creates, identifier(effect.Ref))
			}
			if effect.Kind == verify.CreateEffect || effect.Kind == verify.SetStateEffect {
				stateUpdates = append(stateUpdates, effect)
			}
		}
		if len(creates) != 0 {
			variable := existsVariable(entity.Name)
			modified[variable] = struct{}{}
			out.WriteString(indent + "/\\ " + variable + "' = " + variable + " \\union {" + strings.Join(creates, ", ") + "}\n")
		}
		if len(stateUpdates) != 0 {
			variable := stateVariable(entity.Name)
			modified[variable] = struct{}{}
			var updates []string
			for _, effect := range stateUpdates {
				updates = append(updates, fmt.Sprintf("![%s] = %q", identifier(effect.Ref), effect.State))
			}
			out.WriteString(indent + "/\\ " + variable + "' = [" + variable + " EXCEPT " + strings.Join(updates, ", ") + "]\n")
		}
	}
	for _, relation := range g.model.Relations {
		variable := relationVariable(relation.Name)
		expression := variable
		changed := false
		for _, effect := range effects {
			if effect.Relation != relation.Name {
				continue
			}
			tuple := fmt.Sprintf("{<<%s, %s>>}", identifier(effect.Source), identifier(effect.Target))
			switch effect.Kind {
			case verify.AddRelationEffect:
				expression = expression + " \\union " + tuple
				changed = true
			case verify.RemoveRelationEffect:
				expression = "(" + expression + ") \\ " + tuple
				changed = true
			default:
				continue
			}
		}
		if changed {
			modified[variable] = struct{}{}
			out.WriteString(indent + "/\\ " + variable + "' = " + expression + "\n")
		}
	}
	var unchanged []string
	for _, variable := range g.variables() {
		if _, changed := modified[variable]; !changed {
			unchanged = append(unchanged, variable)
		}
	}
	if len(unchanged) != 0 {
		out.WriteString(indent + "/\\ UNCHANGED <<" + strings.Join(unchanged, ", ") + ">>\n")
	}
}

func (g generator) writeNext(out *bytes.Buffer) {
	out.WriteString("Next ==\n")
	for _, action := range g.model.Actions {
		out.WriteString("    \\/ ")
		out.WriteString(g.actionInvocation(action, actionIdentifier(action.Name)))
		out.WriteString("\n")
	}
	if len(g.model.Actions) == 0 {
		out.WriteString("    FALSE\n")
	}
	out.WriteString("\n")
}

func (g generator) writeCanStep(out *bytes.Buffer) {
	out.WriteString("CanStep ==\n")
	for _, action := range g.model.Actions {
		out.WriteString("    \\/ ")
		out.WriteString(g.actionInvocation(action, actionIdentifier(action.Name)+"Enabled"))
		out.WriteString("\n")
	}
	if len(g.model.Actions) == 0 {
		out.WriteString("    FALSE\n")
	}
	out.WriteString("\n")
}

func (g generator) actionInvocation(action verify.Action, operator string) string {
	if len(action.Parameters) == 0 {
		return operator
	}
	var declarations []string
	for _, parameter := range action.Parameters {
		declarations = append(declarations, fmt.Sprintf("%s \\in %s", identifier(parameter.Name), entityConstant(parameter.Type)))
	}
	return "\\E " + strings.Join(declarations, ", ") + ": " + operator + "(" + strings.Join(parameterNames(action.Parameters), ", ") + ")"
}

func (g generator) writeSafety(out *bytes.Buffer) {
	inductive := []string{"TypeOK"}
	for _, relation := range g.model.Relations {
		inductive = append(inductive, cardinalityIdentifier(relation.Name))
	}
	var declared []string
	for _, property := range g.model.Properties {
		if property.Kind == verify.SafetyProperty {
			inductive = append(inductive, propertyIdentifier(property.Name))
			if !property.Strengthening {
				declared = append(declared, propertyIdentifier(property.Name))
			}
		}
	}
	writeNamedConjunction(out, "InductiveInvariant", inductive)
	writeNamedConjunction(out, "DeclaredSafety", declared)
	out.WriteString("Safety == InductiveInvariant /\\ DeclaredSafety\n")
	out.WriteString("\n")
}

func writeNamedConjunction(out *bytes.Buffer, name string, clauses []string) {
	out.WriteString(name + " ==\n")
	if len(clauses) == 0 {
		clauses = []string{"TRUE"}
	}
	writeClauses(out, clauses)
}

func (g generator) writeQuiescentSafety(out *bytes.Buffer) {
	var properties []string
	for _, property := range g.model.Properties {
		if property.Kind == verify.QuiescentProperty {
			properties = append(properties, propertyIdentifier(property.Name))
		}
	}
	if len(properties) == 0 {
		out.WriteString("QuiescentSafety == TRUE\n\n")
		return
	}
	if len(properties) == 1 {
		out.WriteString("QuiescentSafety == CanStep \\/ " + properties[0] + "\n\n")
		return
	}
	out.WriteString("QuiescentSafety ==\n    \\/ CanStep\n    \\/ /\\ ")
	out.WriteString(strings.Join(properties, "\n       /\\ "))
	out.WriteString("\n\n")
}

func (g generator) expr(expr verify.Expr) string {
	switch expr.Op {
	case verify.TrueExpr:
		return "TRUE"
	case verify.NotExpr:
		return "~(" + g.expr(expr.Args[0]) + ")"
	case verify.AndExpr:
		return "(" + joinExpr(g, expr.Args, " /\\ ") + ")"
	case verify.OrExpr:
		return "(" + joinExpr(g, expr.Args, " \\/ ") + ")"
	case verify.ImpliesExpr:
		return "(" + g.expr(expr.Args[0]) + " => " + g.expr(expr.Args[1]) + ")"
	case verify.EntityExistsExpr:
		return identifier(expr.Ref) + " \\in " + existsVariable(expr.Entity)
	case verify.StateIsExpr:
		return stateVariable(expr.Entity) + "[" + identifier(expr.Ref) + "] = " + fmt.Sprintf("%q", expr.State)
	case verify.RelationHasExpr:
		return "<<" + identifier(expr.Source) + ", " + identifier(expr.Target) + ">> \\in " + relationVariable(expr.Relation)
	case verify.ForAllExpr:
		name := identifier(expr.Var)
		return fmt.Sprintf("\\A %s \\in %s: %s \\in %s => (%s)", name, entityConstant(expr.Entity), name, existsVariable(expr.Entity), g.expr(expr.Args[0]))
	case verify.ExistsExpr:
		name := identifier(expr.Var)
		return fmt.Sprintf("\\E %s \\in %s: %s \\in %s /\\ (%s)", name, entityConstant(expr.Entity), name, existsVariable(expr.Entity), g.expr(expr.Args[0]))
	default:
		return "FALSE"
	}
}

func (g generator) config() string {
	var out bytes.Buffer
	out.WriteString("SPECIFICATION Spec\n")
	out.WriteString("CHECK_DEADLOCK FALSE\n")
	out.WriteString("INVARIANT TypeOK\n")
	for _, relation := range g.model.Relations {
		out.WriteString("INVARIANT " + cardinalityIdentifier(relation.Name) + "\n")
	}
	for _, property := range g.model.Properties {
		if property.Kind == verify.SafetyProperty {
			out.WriteString("INVARIANT " + propertyIdentifier(property.Name) + "\n")
		}
	}
	out.WriteString("INVARIANT QuiescentSafety\n")
	for _, entity := range g.model.Entities {
		out.WriteString("CONSTANT ")
		out.WriteString(entityConstant(entity.Name))
		out.WriteString(" = ")
		out.WriteString(stringSet(entity.IDs))
		out.WriteString("\n")
	}
	return out.String()
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

func writeClauses(out *bytes.Buffer, clauses []string) {
	for _, clause := range clauses {
		out.WriteString("    /\\ ")
		out.WriteString(clause)
		out.WriteString("\n")
	}
}

func writeParameters(out *bytes.Buffer, parameters []string) {
	if len(parameters) != 0 {
		out.WriteString("(")
		out.WriteString(strings.Join(parameters, ", "))
		out.WriteString(")")
	}
}

func writeArguments(out *bytes.Buffer, parameters []string) {
	writeParameters(out, parameters)
}

func parameterNames(parameters []verify.Parameter) []string {
	result := make([]string, len(parameters))
	for index, parameter := range parameters {
		result[index] = identifier(parameter.Name)
	}
	return result
}

func stateNames(entity verify.EntityType) []string {
	result := make([]string, len(entity.States))
	for index, state := range entity.States {
		result[index] = state.Name
	}
	return result
}

func stringSet(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = fmt.Sprintf("%q", value)
	}
	return "{" + strings.Join(quoted, ", ") + "}"
}

func joinExpr(g generator, expressions []verify.Expr, separator string) string {
	result := make([]string, len(expressions))
	for index, expression := range expressions {
		result[index] = g.expr(expression)
	}
	return strings.Join(result, separator)
}

func entityConstant(name string) string        { return identifier(name) + "IDs" }
func existsVariable(name string) string        { return "exists_" + identifier(name) }
func stateVariable(name string) string         { return "state_" + identifier(name) }
func relationVariable(name string) string      { return "relation_" + identifier(name) }
func cardinalityIdentifier(name string) string { return "Cardinality_" + identifier(name) }
func propertyIdentifier(name string) string    { return identifier(name) }

func actionIdentifier(name string) string {
	value := identifier(name)
	if value == "" {
		return "Action"
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

// ActionIdentifier returns the generated TLA+ operator name for a source action.
func ActionIdentifier(name string) string {
	return actionIdentifier(name)
}

// PropertyIdentifier returns the generated TLA+ operator name for a source property.
func PropertyIdentifier(name string) string {
	return propertyIdentifier(name)
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
		result = append([]rune{'X', '_'}, result...)
	}
	return string(result)
}

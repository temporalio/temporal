package ivy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"go.temporal.io/server/common/testing/umpire/verify"
)

type Diagnostic struct {
	Construct string `json:"construct"`
	Reason    string `json:"reason"`
}

func Generate(model verify.Model) (map[string][]byte, []Diagnostic, error) {
	if err := verify.Validate(model); err != nil {
		return nil, nil, fmt.Errorf("generate Ivy: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return nil, nil, fmt.Errorf("generate Ivy: %w", err)
	}
	g := generator{model: model}
	if err := g.validateIdentifiers(); err != nil {
		return nil, nil, err
	}
	source, diagnostics := g.source()
	return map[string][]byte{"Umpire.ivy": []byte(source)}, diagnostics, nil
}

func TraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	if err := verify.Validate(model); err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate Ivy trace vocabulary: %w", err)
	}
	model, err := canonicalModel(model)
	if err != nil {
		return verify.TraceVocabulary{}, fmt.Errorf("generate Ivy trace vocabulary: %w", err)
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
			vocabulary.Identities[entityID(id)] = id
		}
		for _, state := range entity.States {
			vocabulary.States[stateID(entity.Name, state.Name)] = state.Name
		}
	}
	for _, relation := range model.Relations {
		name := identifier(relation.Name)
		vocabulary.Relations[relationName(relation.Name)] = relation.Name
		vocabulary.Properties["relation_"+name+"_endpoints"] = []string{"relation " + relation.Name + " endpoints"}
		if relation.SourceCardinality == verify.One {
			vocabulary.Properties["cardinality_"+name+"_source"] = []string{"relation " + relation.Name + " source cardinality"}
		}
		if relation.TargetCardinality == verify.One {
			vocabulary.Properties["cardinality_"+name+"_target"] = []string{"relation " + relation.Name + " target cardinality"}
		}
	}
	for _, action := range model.Actions {
		actionName := identifier(action.Name)
		vocabulary.Actions[actionName] = action.Name
		bindings := make(map[string]string, len(action.Parameters))
		for _, parameter := range action.Parameters {
			bindings[identifier(parameter.Name)] = parameter.Name
		}
		vocabulary.Bindings[actionName] = bindings
	}
	for _, property := range model.Properties {
		vocabulary.Properties[identifier(property.Name)] = []string{property.Name}
	}
	return vocabulary, nil
}

type generator struct {
	model verify.Model
}

func (g generator) source() (string, []Diagnostic) {
	var out bytes.Buffer
	out.WriteString("#lang ivy1.7\n\n")
	out.WriteString("# Generated from the Umpire verification snapshot. Do not edit.\n\n")
	for _, entity := range g.model.Entities {
		fmt.Fprintf(&out, "type %s\n", identifier(entity.Name))
		if len(entity.States) != 0 {
			states := make([]string, len(entity.States))
			for index, state := range entity.States {
				states[index] = stateID(entity.Name, state.Name)
			}
			fmt.Fprintf(&out, "type %s = {%s}\n", stateType(entity.Name), strings.Join(states, ","))
		}
		for _, value := range entity.IDs {
			fmt.Fprintf(&out, "individual %s:%s\n", entityID(value), identifier(entity.Name))
		}
		fmt.Fprintf(&out, "relation %s(X:%s)\n", existsName(entity.Name), identifier(entity.Name))
		if len(entity.States) != 0 {
			fmt.Fprintf(&out, "function %s(X:%s):%s\n", stateName(entity.Name), identifier(entity.Name), stateType(entity.Name))
		}
		out.WriteString("\n")
	}
	for _, relation := range g.model.Relations {
		fmt.Fprintf(&out, "relation %s(X:%s, Y:%s)\n", relationName(relation.Name), identifier(relation.Source), identifier(relation.Target))
	}
	out.WriteString("\nafter init {\n")
	var initializers []string
	for _, entity := range g.model.Entities {
		initializers = append(initializers, fmt.Sprintf("%s(X) := false", existsName(entity.Name)))
		if len(entity.States) != 0 {
			initializers = append(initializers, fmt.Sprintf("%s(X) := %s", stateName(entity.Name), stateID(entity.Name, entity.Initial)))
		}
		for _, value := range entity.InitiallyExists {
			initializers = append(initializers, fmt.Sprintf("%s(%s) := true", existsName(entity.Name), entityID(value)))
		}
	}
	for _, relation := range g.model.Relations {
		initializers = append(initializers, fmt.Sprintf("%s(X,Y) := false", relationName(relation.Name)))
	}
	g.writeStatements(&out, initializers, "    ")
	out.WriteString("}\n\n")
	for _, action := range g.model.Actions {
		g.writeAction(&out, action)
	}
	for _, relation := range g.model.Relations {
		g.writeRelationInvariants(&out, relation)
	}
	var diagnostics []Diagnostic
	for _, property := range g.model.Properties {
		if property.Kind != verify.SafetyProperty {
			reason := "Ivy generation supports inductive safety properties only"
			diagnostics = append(diagnostics, Diagnostic{Construct: "property " + property.Name, Reason: reason})
			fmt.Fprintf(&out, "# unsupported property %s: %s\n", property.Name, reason)
			continue
		}
		fmt.Fprintf(&out, "invariant [%s] %s\n", identifier(property.Name), g.expr(property.Expr, map[string]string{}))
	}
	out.WriteString("\n")
	for _, action := range g.model.Actions {
		fmt.Fprintf(&out, "export %s\n", identifier(action.Name))
	}
	return out.String(), diagnostics
}

func (g generator) writeAction(out *bytes.Buffer, action verify.Action) {
	parameters := make([]string, len(action.Parameters))
	bindings := make(map[string]string, len(action.Parameters))
	for index, parameter := range action.Parameters {
		name := identifier(parameter.Name)
		parameters[index] = name + ":" + identifier(parameter.Type)
		bindings[parameter.Name] = name
	}
	fmt.Fprintf(out, "action %s(%s) = {\n", identifier(action.Name), strings.Join(parameters, ","))
	for _, parameter := range action.Parameters {
		condition := existsName(parameter.Type) + "(" + bindings[parameter.Name] + ")"
		if parameter.Binding == verify.FreshBinding {
			condition = "~" + condition
		}
		fmt.Fprintf(out, "    require %s;\n", condition)
	}
	for _, pair := range verify.DistinctFreshParameterPairs(action.Parameters) {
		fmt.Fprintf(out, "    require %s ~= %s;\n", bindings[pair[0].Name], bindings[pair[1].Name])
	}
	if action.Guard.Op != "" && action.Guard.Op != verify.TrueExpr {
		fmt.Fprintf(out, "    require %s;\n", g.expr(action.Guard, bindings))
	}
	if len(action.Branches) == 0 {
		g.writeEffects(out, action.Effects, bindings, "    ")
	} else {
		g.writeBranch(out, action, bindings, 0, "    ")
	}
	out.WriteString("}\n\n")
}

func (g generator) writeBranch(out *bytes.Buffer, action verify.Action, bindings map[string]string, index int, indent string) {
	branch := action.Branches[index]
	if index == len(action.Branches)-1 {
		g.writeEffects(out, appendEffects(action.Effects, branch.Effects), bindings, indent)
		return
	}
	out.WriteString(indent + "if * {\n")
	g.writeEffects(out, appendEffects(action.Effects, branch.Effects), bindings, indent+"    ")
	out.WriteString(indent + "} else {\n")
	g.writeBranch(out, action, bindings, index+1, indent+"    ")
	out.WriteString(indent + "};\n")
}

func (g generator) writeEffects(out *bytes.Buffer, effects []verify.Effect, bindings map[string]string, indent string) {
	statements := make([]string, 0, len(effects)*2)
	for _, effect := range effects {
		switch effect.Kind {
		case verify.CreateEffect:
			statements = append(statements,
				fmt.Sprintf("%s(%s) := true", existsName(effect.Entity), bindings[effect.Ref]),
				fmt.Sprintf("%s(%s) := %s", stateName(effect.Entity), bindings[effect.Ref], stateID(effect.Entity, effect.State)),
			)
		case verify.SetStateEffect:
			statements = append(statements, fmt.Sprintf("%s(%s) := %s", stateName(effect.Entity), bindings[effect.Ref], stateID(effect.Entity, effect.State)))
		case verify.AddRelationEffect:
			statements = append(statements, fmt.Sprintf("%s(%s,%s) := true", relationName(effect.Relation), bindings[effect.Source], bindings[effect.Target]))
		case verify.RemoveRelationEffect:
			statements = append(statements, fmt.Sprintf("%s(%s,%s) := false", relationName(effect.Relation), bindings[effect.Source], bindings[effect.Target]))
		default:
			panic(fmt.Sprintf("validated model has unknown effect kind %q", effect.Kind))
		}
	}
	g.writeStatements(out, statements, indent)
}

func (g generator) writeStatements(out *bytes.Buffer, statements []string, indent string) {
	if len(statements) == 0 {
		out.WriteString(indent + "# no state change\n")
		return
	}
	for index, statement := range statements {
		out.WriteString(indent + statement)
		if index != len(statements)-1 {
			out.WriteString(";")
		}
		out.WriteString("\n")
	}
}

func (g generator) writeRelationInvariants(out *bytes.Buffer, relation verify.Relation) {
	name := identifier(relation.Name)
	fmt.Fprintf(out, "invariant [relation_%s_endpoints] forall S:%s,T:%s. %s(S,T) -> %s(S) & %s(T)\n",
		name, identifier(relation.Source), identifier(relation.Target), relationName(relation.Name), existsName(relation.Source), existsName(relation.Target))
	if relation.SourceCardinality == verify.One {
		fmt.Fprintf(out, "invariant [cardinality_%s_source] forall S:%s,T1:%s,T2:%s. %s(S,T1) & %s(S,T2) -> T1 = T2\n",
			name, identifier(relation.Source), identifier(relation.Target), identifier(relation.Target), relationName(relation.Name), relationName(relation.Name))
	}
	if relation.TargetCardinality == verify.One {
		fmt.Fprintf(out, "invariant [cardinality_%s_target] forall S1:%s,S2:%s,T:%s. %s(S1,T) & %s(S2,T) -> S1 = S2\n",
			name, identifier(relation.Source), identifier(relation.Source), identifier(relation.Target), relationName(relation.Name), relationName(relation.Name))
	}
}

func (g generator) expr(expression verify.Expr, bindings map[string]string) string {
	switch expression.Op {
	case "", verify.TrueExpr:
		return "true"
	case verify.NotExpr:
		return "~(" + g.expr(expression.Args[0], bindings) + ")"
	case verify.AndExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " & ") + ")"
	case verify.OrExpr:
		return "(" + g.joinExpr(expression.Args, bindings, " | ") + ")"
	case verify.ImpliesExpr:
		return "(" + g.expr(expression.Args[0], bindings) + " -> " + g.expr(expression.Args[1], bindings) + ")"
	case verify.EntityExistsExpr:
		return existsName(expression.Entity) + "(" + bindings[expression.Ref] + ")"
	case verify.StateIsExpr:
		return stateName(expression.Entity) + "(" + bindings[expression.Ref] + ") = " + stateID(expression.Entity, expression.State)
	case verify.RelationHasExpr:
		return relationName(expression.Relation) + "(" + bindings[expression.Source] + "," + bindings[expression.Target] + ")"
	case verify.ForAllExpr, verify.ExistsExpr:
		name := "Q_" + identifier(expression.Var)
		inner := cloneBindings(bindings)
		inner[expression.Var] = name
		body := g.expr(expression.Args[0], inner)
		exists := existsName(expression.Entity) + "(" + name + ")"
		if expression.Op == verify.ForAllExpr {
			return "(forall " + name + ":" + identifier(expression.Entity) + ". " + exists + " -> (" + body + "))"
		}
		return "~(forall " + name + ":" + identifier(expression.Entity) + ". ~(" + exists + " & (" + body + ")))"
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

func (g generator) validateIdentifiers() error {
	seen := map[string]string{}
	check := func(source, target string) error {
		if previous, found := seen[target]; found && previous != source {
			return fmt.Errorf("generate Ivy: %q and %q normalize to identifier %q", previous, source, target)
		}
		seen[target] = source
		return nil
	}
	for _, entity := range g.model.Entities {
		if err := check(entity.Name, identifier(entity.Name)); err != nil {
			return err
		}
		for _, id := range entity.IDs {
			if err := check(entity.Name+" identity "+id, entityID(id)); err != nil {
				return err
			}
		}
		for _, state := range entity.States {
			if err := check(entity.Name+"."+state.Name, stateID(entity.Name, state.Name)); err != nil {
				return err
			}
		}
	}
	for _, relation := range g.model.Relations {
		if err := check(relation.Name, relationName(relation.Name)); err != nil {
			return err
		}
	}
	for _, action := range g.model.Actions {
		if err := check(action.Name, identifier(action.Name)); err != nil {
			return err
		}
		parameters := map[string]string{}
		for _, parameter := range action.Parameters {
			generated := identifier(parameter.Name)
			if previous, duplicate := parameters[generated]; duplicate {
				return fmt.Errorf(
					"generate Ivy: action %q parameters %q and %q normalize to parameter identifier %q",
					action.Name, previous, parameter.Name, generated,
				)
			}
			parameters[generated] = parameter.Name
		}
	}
	for _, property := range g.model.Properties {
		if err := check(property.Name, identifier(property.Name)); err != nil {
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

func identifier(value string) string {
	var result []rune
	for _, current := range value {
		if unicode.IsLetter(current) || unicode.IsDigit(current) || current == '_' {
			result = append(result, unicode.ToLower(current))
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

// ActionIdentifier returns the generated Ivy action name for a source action.
func ActionIdentifier(name string) string {
	return identifier(name)
}

// PropertyIdentifier returns the generated Ivy invariant name for a source property.
func PropertyIdentifier(name string) string {
	return identifier(name)
}

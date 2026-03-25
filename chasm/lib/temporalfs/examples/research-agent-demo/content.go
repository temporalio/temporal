package main

import (
	"fmt"
	"math/rand"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Source represents a generated research source document.
type Source struct {
	Filename string
	Content  []byte
}

// Pools of template fragments for realistic content generation.
var (
	authorLastNames = []string{
		"Chen", "Patel", "Smith", "Garcia", "Kim", "Johnson", "Williams",
		"Mueller", "Nakamura", "Silva", "Brown", "Lee", "Anderson", "Taylor",
		"Wang", "Martinez", "Thompson", "Yamamoto", "Petrov", "Okafor",
	}

	authorFirstNames = []string{
		"A.", "B.", "C.", "D.", "E.", "F.", "G.", "H.", "I.", "J.",
		"K.", "L.", "M.", "N.", "O.", "P.", "Q.", "R.", "S.", "T.",
	}

	journalNames = []string{
		"Nature", "Science", "PNAS", "Physical Review Letters",
		"IEEE Transactions", "ACM Computing Surveys", "The Lancet",
		"Cell", "arXiv preprint", "Annual Review",
	}

	keyPointPrefixes = []string{
		"Demonstrates that", "Proposes a novel framework for",
		"Provides evidence suggesting", "Introduces a scalable approach to",
		"Challenges the conventional view of", "Extends prior work on",
		"Establishes a theoretical foundation for", "Presents experimental results on",
		"Surveys recent advances in", "Identifies key limitations of",
	}

	findingPrefixes = []string{
		"Recent advances in %s suggest",
		"The intersection of %s and adjacent fields reveals",
		"A growing body of evidence indicates that %s",
		"Computational approaches to %s have shown",
		"Cross-disciplinary analysis of %s demonstrates",
		"Emerging trends in %s point toward",
		"The theoretical foundations of %s are shifting due to",
		"Practical applications of %s are increasingly driven by",
	}

	verdicts       = []string{"Confirmed", "Partially Confirmed", "Needs Context", "Unverified", "Confirmed"}
	strengthAdjs   = []string{"comprehensive", "rigorous", "innovative", "well-structured", "thorough"}
	weaknessAdjs   = []string{"limited", "narrow", "incomplete", "surface-level", "brief"}
	reviewScores   = []string{"7.0", "7.5", "8.0", "8.5", "9.0"}
)

func generateSources(topic string, seed int64) []Source {
	r := rand.New(rand.NewSource(seed))
	count := 3 + r.Intn(3) // 3-5 sources
	sources := make([]Source, count)
	baseYear := 2015 + r.Intn(5)

	for i := range count {
		year := baseYear + i*2
		lastName := authorLastNames[r.Intn(len(authorLastNames))]
		firstName := authorFirstNames[r.Intn(len(authorFirstNames))]
		journal := journalNames[r.Intn(len(journalNames))]

		title := fmt.Sprintf("On the Foundations of %s: Perspective %d", topic, i+1)
		slug := fmt.Sprintf("%s-%d", strings.ToLower(strings.ReplaceAll(lastName, " ", "-")), year)

		numPoints := 2 + r.Intn(3)
		var points strings.Builder
		for j := range numPoints {
			prefix := keyPointPrefixes[r.Intn(len(keyPointPrefixes))]
			points.WriteString(fmt.Sprintf("%d. %s %s in the context of modern research.\n", j+1, prefix, strings.ToLower(topic)))
		}

		content := fmt.Sprintf(`# %s

**Authors:** %s %s et al.
**Published:** %s (%d)
**DOI:** 10.1234/example.%d.%d

## Abstract

This paper examines recent developments in %s, with particular focus on
emerging methodologies and their implications for the field. Through a
combination of theoretical analysis and empirical evaluation, we present
findings that advance the current understanding of %s.

## Key Points

%s
## Citation Impact

Cited by %d papers as of 2025. H-index contribution: %d.
`, title, firstName, lastName, journal, year, year, i+1,
			strings.ToLower(topic), strings.ToLower(topic),
			points.String(), 50+r.Intn(200), 5+r.Intn(15))

		sources[i] = Source{
			Filename: slug + ".md",
			Content:  []byte(content),
		}
	}
	return sources
}

func generateSummary(topic string, sourceNames []string, seed int64) []byte {
	r := rand.New(rand.NewSource(seed + 100))

	var sourceList strings.Builder
	for _, name := range sourceNames {
		sourceList.WriteString(fmt.Sprintf("- %s\n", name))
	}

	numFindings := 3 + r.Intn(3)
	var findings strings.Builder
	for i := range numFindings {
		prefix := findingPrefixes[r.Intn(len(findingPrefixes))]
		findings.WriteString(fmt.Sprintf("%d. %s new possibilities for practical application.\n",
			i+1, fmt.Sprintf(prefix, strings.ToLower(topic))))
	}

	return []byte(fmt.Sprintf(`# Research Summary — %s

## Sources Analyzed

%s
## Key Findings

%s
## Cross-Cutting Themes

1. **Scalability Challenges**: Multiple sources highlight the difficulty of scaling
   current approaches to %s beyond laboratory conditions.
2. **Interdisciplinary Convergence**: The field is increasingly drawing from adjacent
   disciplines, creating new hybrid methodologies.
3. **Data Requirements**: All reviewed approaches require significant high-quality
   data, raising questions about accessibility and bias.

## Open Questions

- How will regulatory frameworks adapt to advances in %s?
- What are the long-term societal implications of widespread adoption?
- Can current theoretical models account for edge cases observed in practice?
- What role will open-source tools play in democratizing access?
`, topic, sourceList.String(), findings.String(),
		strings.ToLower(topic), strings.ToLower(topic)))
}

func generateFactCheck(topic string, seed int64) []byte {
	r := rand.New(rand.NewSource(seed + 200))

	numClaims := 5 + r.Intn(4)
	var rows strings.Builder
	for i := range numClaims {
		verdict := verdicts[r.Intn(len(verdicts))]
		rows.WriteString(fmt.Sprintf("| %s has shown %d%% improvement in key metrics | Source %d | %s | Based on %d-year longitudinal data |\n",
			topic, 10+r.Intn(80), r.Intn(5)+1, verdict, 1+r.Intn(10)))
		_ = i
	}

	return []byte(fmt.Sprintf(`# Fact Check — %s

## Verification Methodology

Each claim from the research summary was cross-referenced against the original
source material and, where possible, validated against independent datasets
and peer-reviewed meta-analyses.

## Results

| Claim | Source | Verdict | Notes |
|-------|--------|---------|-------|
%s
## Summary

- **Confirmed**: %d claims fully supported by evidence
- **Partially Confirmed**: %d claims with caveats or limited scope
- **Needs Context**: %d claims require additional qualification
- **Unverified**: %d claims could not be independently verified

Overall confidence level: **%.1f/10**
`, topic, rows.String(),
		2+r.Intn(3), 1+r.Intn(2), r.Intn(2), r.Intn(2),
		7.0+float64(r.Intn(20))/10.0))
}

func generateFinalReport(topic string, seed int64) []byte {
	r := rand.New(rand.NewSource(seed + 300))

	numRecs := 3 + r.Intn(3)
	var recs strings.Builder
	recPrefixes := []string{
		"Invest in", "Monitor developments in", "Establish partnerships for",
		"Develop internal capabilities in", "Commission further research on",
		"Create a task force to evaluate", "Begin pilot programs for",
	}
	for i := range numRecs {
		prefix := recPrefixes[r.Intn(len(recPrefixes))]
		recs.WriteString(fmt.Sprintf("%d. %s %s to maintain competitive advantage.\n", i+1, prefix, strings.ToLower(topic)))
	}

	return []byte(fmt.Sprintf(`# Final Report — %s

## Executive Summary

This report synthesizes findings from %d primary sources, cross-referenced
through independent fact-checking, to provide actionable intelligence on
the current state and future trajectory of %s.

The field is at a critical inflection point. Recent breakthroughs have
shortened the timeline for practical applications from decades to years,
while simultaneously raising important questions about governance,
accessibility, and unintended consequences.

## Methodology

1. **Source Collection**: Gathered %d peer-reviewed papers and preprints (2015-2025)
2. **Synthesis**: Identified cross-cutting themes and convergent findings
3. **Fact-Checking**: Independently verified %d%% of quantitative claims
4. **Peer Review**: Internal review by domain experts

## Detailed Findings

### Current State of the Art

The leading approaches to %s have evolved significantly over the past five
years. Key advances include improved scalability, reduced computational
requirements, and novel theoretical frameworks that unify previously
disparate research threads.

### Emerging Trends

Three trends are reshaping the landscape:
- **Democratization**: Open-source tooling is lowering barriers to entry
- **Convergence**: Cross-disciplinary approaches are yielding outsized results
- **Regulation**: Governments are beginning to establish frameworks for responsible development

### Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Technical plateau | Medium | High | Diversify research portfolio |
| Regulatory barriers | Medium | Medium | Engage with policymakers early |
| Talent shortage | High | High | Invest in training programs |
| Ethical concerns | Medium | High | Establish ethics review board |

## Recommendations

%s
## Conclusion

%s represents a significant opportunity. Organizations that invest now
in building capabilities, forming strategic partnerships, and engaging
with the broader ecosystem will be best positioned to capture value as
the field matures.

---
*Report generated by AI Research Agent • Powered by TemporalFS*
`, topic,
		3+r.Intn(3), strings.ToLower(topic),
		3+r.Intn(3), 70+r.Intn(25),
		strings.ToLower(topic),
		recs.String(), topic))
}

func generatePeerReview(topic string, seed int64) []byte {
	r := rand.New(rand.NewSource(seed + 400))

	strengthAdj := strengthAdjs[r.Intn(len(strengthAdjs))]
	weaknessAdj := weaknessAdjs[r.Intn(len(weaknessAdjs))]
	score := reviewScores[r.Intn(len(reviewScores))]

	titleCase := cases.Title(language.English)
	return []byte(fmt.Sprintf(`# Peer Review — %s

## Reviewer Assessment

### Strengths

1. **%s coverage** of the source material, drawing from multiple
   high-impact publications spanning the last decade.
2. The fact-checking methodology adds credibility and transparency
   to the research process.
3. Clear progression from data gathering through analysis to
   actionable recommendations.
4. Risk assessment matrix provides practical decision-making support.

### Weaknesses

1. **%s treatment** of some counterarguments and alternative
   viewpoints in the field.
2. Some claims in the summary could benefit from more specific
   quantitative backing.
3. The recommendation section could be more specific about
   implementation timelines and resource requirements.

### Missing Coverage

- Industry perspective and commercial applications
- Comparison with competing approaches outside the primary literature
- Long-term (10+ year) trend analysis
- Geographic and cultural variations in adoption

### Suggestions for Improvement

1. Include a dedicated section on limitations and potential biases
2. Add a glossary of technical terms for non-specialist readers
3. Provide more granular confidence intervals for key claims
4. Consider adding case studies from early adopters

## Overall Score: %s/10

The report provides a solid foundation for understanding %s.
With the suggested improvements, it would serve as a comprehensive
reference for both technical and strategic decision-makers.

---
*Peer review conducted by AI Review Agent • Powered by TemporalFS*
`, topic, titleCase.String(strengthAdj), titleCase.String(weaknessAdj), score, strings.ToLower(topic)))
}

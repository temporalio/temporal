package slack

import "strings"

const (
	maxHeaderTextCharacters  = 150
	maxSectionTextCharacters = 3000
	maxFieldTextCharacters   = 2000
	truncationSuffix         = "\n\n...(truncated due to length)"
)

// Message represents a Slack Block Kit message.
type Message struct {
	Text   string  `json:"text"`
	Blocks []Block `json:"blocks"`
}

// Block represents a block in a Slack message.
type Block struct {
	Type   string `json:"type"`
	Text   *Text  `json:"text,omitempty"`
	Fields []Text `json:"fields,omitempty"`
}

// Text represents text content in a Slack block.
type Text struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NewMessage creates an empty Slack message with fallback notification text.
func NewMessage(text string) *Message {
	return &Message{Text: text}
}

// AddHeader appends a plain-text header block.
func (m *Message) AddHeader(text string) {
	m.Blocks = append(m.Blocks, Block{
		Type: "header",
		Text: &Text{
			Type: "plain_text",
			Text: truncate(text, maxHeaderTextCharacters),
		},
	})
}

// AddSection appends a Markdown section block.
func (m *Message) AddSection(text string) {
	m.Blocks = append(m.Blocks, Block{
		Type: "section",
		Text: &Text{
			Type: "mrkdwn",
			Text: truncate(text, maxSectionTextCharacters),
		},
	})
}

// AddFields appends a section containing Markdown fields.
func (m *Message) AddFields(fields ...string) {
	if len(fields) == 0 {
		return
	}

	textFields := make([]Text, 0, len(fields))
	for _, field := range fields {
		textFields = append(textFields, Text{
			Type: "mrkdwn",
			Text: truncate(field, maxFieldTextCharacters),
		})
	}
	m.Blocks = append(m.Blocks, Block{
		Type:   "section",
		Fields: textFields,
	})
}

// RenderMarkdown renders the message blocks as Markdown.
func (m *Message) RenderMarkdown() string {
	var sb strings.Builder
	for _, block := range m.Blocks {
		if block.Text != nil {
			sb.WriteString(block.Text.Text)
			sb.WriteString("\n\n")
		}
		for _, field := range block.Fields {
			sb.WriteString(field.Text)
			sb.WriteString("\n")
		}
		if len(block.Fields) > 0 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func truncate(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}

	suffix := []rune(truncationSuffix)
	return string(runes[:limit-len(suffix)]) + truncationSuffix
}

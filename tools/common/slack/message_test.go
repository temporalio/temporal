package slack

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestMessageBlocks(t *testing.T) {
	message := NewMessage("fallback")
	message.AddHeader("Report")
	message.AddSection("*Summary:* complete")
	message.AddFields("*Runs:*\n10", "*Failures:*\n2")

	require.Equal(t, &Message{
		Text: "fallback",
		Blocks: []Block{
			{
				Type: "header",
				Text: &Text{
					Type: "plain_text",
					Text: "Report",
				},
			},
			{
				Type: "section",
				Text: &Text{
					Type: "mrkdwn",
					Text: "*Summary:* complete",
				},
			},
			{
				Type: "section",
				Fields: []Text{
					{
						Type: "mrkdwn",
						Text: "*Runs:*\n10",
					},
					{
						Type: "mrkdwn",
						Text: "*Failures:*\n2",
					},
				},
			},
		},
	}, message)
	require.Equal(t, "Report\n\n*Summary:* complete\n\n*Runs:*\n10\n*Failures:*\n2\n\n", message.RenderMarkdown())
}

func TestMessageTruncatesBlockText(t *testing.T) {
	message := NewMessage("fallback")
	message.AddHeader(strings.Repeat("h", maxHeaderTextCharacters+1))
	message.AddSection(strings.Repeat("界", maxSectionTextCharacters+1))
	message.AddFields(strings.Repeat("f", maxFieldTextCharacters+1))

	require.Len(t, []rune(message.Blocks[0].Text.Text), maxHeaderTextCharacters)
	require.Len(t, []rune(message.Blocks[1].Text.Text), maxSectionTextCharacters)
	require.Len(t, []rune(message.Blocks[2].Fields[0].Text), maxFieldTextCharacters)
	require.True(t, utf8.ValidString(message.Blocks[1].Text.Text))
	require.True(t, strings.HasSuffix(message.Blocks[1].Text.Text, truncationSuffix))
}

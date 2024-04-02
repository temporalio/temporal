.DEFAULT_GOAL := update-diagrams
COLOR := "\e[1;36m%s\e[0m\n"
DIAGRAMS := $(wildcard ./docs/_assets/*.d2)

install-d2:
	@printf $(COLOR) "Install d2..."
	@go install oss.terrastruct.com/d2@v0.6.3

update-diagrams: install-d2
	@printf $(COLOR) "Update diagrams"
	$(foreach DIAGRAM, $(DIAGRAMS), d2 --layout elk --theme 7 --dark-theme 200 $(DIAGRAM) $(NEWLINE))

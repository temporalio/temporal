.DEFAULT_GOAL := update-diagrams
COLOR := "\e[1;36m%s\e[0m\n"
ASSETS_DIR := _assets
D2_GLOB := $(ASSETS_DIR)/*.d2
DIAGRAMS := $(wildcard $(D2_GLOB))
D2_FLAGS := --theme 7 --dark-theme 200

install-d2:
	@printf $(COLOR) "Install d2..."
	@go install oss.terrastruct.com/d2@v0.7.1

update-diagrams: install-d2
	@printf $(COLOR) "Update diagrams"
	$(foreach DIAGRAM, $(DIAGRAMS), $(shell d2 $(D2_FLAGS) $(DIAGRAM) $(NEWLINE)))

install-reflex:
	@go install github.com/cespare/reflex@v0.3.2

watch-diagrams: install-d2 install-reflex
	reflex -r '^$(ASSETS_DIR)/.*\.d2$$' -- d2 $(D2_FLAGS) '{}'

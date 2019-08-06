REORDER_PYTHON_IMPORTS := reorder-python-imports --py3-plus --separate-from-import --separate-relative
PYTHON_FILES = $(shell find cqlmapper/ tests/ -name '*.py')


test:
	docker-compose run cqlmapper nosetests -v

fmt:
	$(REORDER_PYTHON_IMPORTS) --exit-zero-even-if-changed $(PYTHON_FILES)
	black cqlmapper/ tests/

lint:
	$(REORDER_PYTHON_IMPORTS) --diff-only $(PYTHON_FILES)
	black --diff --check cqlmapper/ tests/

.PHONY: test fmt lint

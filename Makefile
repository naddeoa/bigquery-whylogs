NAME=test
METHOD=
JOB_NAME=method-$(METHOD)--$(NAME)
BUCKET=gs://$(USER)_beam/$(JOB_NAME)
TMP_BUCKET=$(BUCKET)/tmp
REGION=us-west1 # us-west2, us-central1

.PHONY: run 2 default

default:help

1:METHOD=1
1:run ## Attempts to use GroupIntoBatches to feed lists of rows into the combiner.

1-control:METHOD=1-control
1-control:run ## Attempts to use GroupIntoBatches to feed lists of rows into the combiner.

2:METHOD=2
2:run ## Does profiling element-wise in a map step and reduces into views

3:METHOD=3
3:run ## Hello world reduce, but very slow

noop:METHOD=noop
noop:run ## Does the same thing 1 does but without any real profiling 

counts:METHOD=counts
counts:run ## Job that computes the count of rows per day timestamp

run:
	python ./test.py --output $(BUCKET) \
	   --temp_location $(TMP_BUCKET) \
	   --job_name $(JOB_NAME) \
	   --runner DataflowRunner \
	   --project whylogs-359820 \
	   --region $(REGION) \
	   --requirements_file=requirements.txt \
	   --num_workers 10 \
	   --method $(METHOD)
	   

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.+) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'

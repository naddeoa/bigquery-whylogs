
The makefile has a few canned targets that you can execute. I hard coded stuff in the source code as Method 1 - 3 and made make targets for them.


```bash
make help
```

This is what its doing.

```python
python ./test.py --output gs://anthony-beam-test/verbose/  --temp_location gs://anthony-beam-test/beam-grouped/tmp/ --runner DataflowRunner  --project whylogs-359820 --region us-west1 --requirements_file=requirements.txt --num_workers 10
```
docs:
	cd docs && make html

test:
	nosetests -v test/

clean:
	rm -fr docs/build/* build/* dist/* .egg slipoframes.egg-info

dist:
	python setup.py test sdist bdist_wheel

.PHONY: clean docs test dist
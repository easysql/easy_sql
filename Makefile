test-coverage:
	export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && \
 		python3 -m coverage run -m unittest discover -s easy_sql -t . -p '*_test.py'
	python3 -m coverage report -m
	- mkdir build
	- rm -r build/coverage
	python3 -m coverage html

unit-test:
	export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && \
 		python3 -m unittest discover -s easy_sql -t . -p '*_test.py'

package-pip:
	python3 -m pip install --upgrade build
	python3 -m build
	# It looks like the build command will remove files in easy_sql directory, restore it after a build
	git restore easy_sql

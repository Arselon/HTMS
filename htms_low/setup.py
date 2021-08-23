import setuptools

with open("README.md.pypi", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    # https://stackoverflow.com/questions/58533084/what-keyword-arguments-does-setuptools-setup-accept
    name="htms_low_api",
    version="2.3.1",
    description="Hyper Table Management System (HTMS) - low level API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arselon/HTMS",
    author="Arslan Aliev",
    author_email="arslanaliev@yahoo.com",
    maintainer="Arslan Aliev",
    maintainer_email="arslanaliev@yahoo.com",	
    license="Apache",
    packages=setuptools.find_packages(), 	
    install_requires=['cage_api>=2.10.0'],
    keywords= [
		'HTMS', 
		'dbms', 
		'API', 
		'low level'
	],
    python_requires=">=3.7",	
    classifiers=[
        "Programming Language :: Python :: 3.7",
	    "Intended Audience :: Developers",	
    ],
)
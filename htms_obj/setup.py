import setuptools

with open("README.md.pypi", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    # https://stackoverflow.com/questions/58533084/what-keyword-arguments-does-setuptools-setup-accept
    name="htms_obj",
    version="2.3.0",
    description="Hyper Table Management System (HTMS) - high level API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arselon/HTMS",
    author="Arslan Aliev",
    author_email="arslanaliev@yahoo.com",
    maintainer="Arslan Aliev",
    maintainer_email="arslanaliev@yahoo.com",	
    license="Apache",
    packages=setuptools.find_packages(), 	
    install_requires=[
		'cage_api>=2.9.0',
		'htms_low_api>=2.3.0',
		'htms_mid_api>=2.3.0'
	],
    keywords= [
		'HTMS', 
		'dbms', 
		'API', 
		'high level',
		'object-htdb mapping',
		'OOP'
	],
    python_requires=">=3.7",	
    classifiers=[
        "Programming Language :: Python :: 3.7",
	    "Intended Audience :: Developers",	
    ],
)
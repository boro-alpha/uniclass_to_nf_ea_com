# UNICLASS bCLEARer Process

The present code is a Python implementation of the [bCLEARer process](https://borosolutions.net/bclearer-approach) developed by [BORO Solutions](https://borosolutions.net/) being applied over the [UNICLASS classification dataset](https://www.thenbs.com/our-tools/uniclass-2015).

The code transparently documents the transformations that arise from the data cleaning and ontological analysis that takes place in the different stages of the process, and it outputs the result in a self-contained folder system divided into sub-folders corresponding to each of the implemented bCLEARer stages and sub-stages.

The bCLEARer process exports the following stages and sub-stages in this sequence:

* **bCLEARer Load Stage**. Exporting to the sub-stage folder:
    * load_4 - loads UNICLASS data tables and adds an identifier (uuid) to each UNICLASS item
* **bCLEARer Evolve Stage**. Exporting to the sub-stage folders:
    * evolve_1 - Exposes the implicit top level UNICLASS items (areas) and adds them to the data tables
    * evolve_2 - Completes the data tables with an extra Area column and populates it
    * evolve_3 - Merges all source tables into one UNICLASS objects table and collects the UNICLASS items namespaces
    * evolve_4 - Exposes implicit parent-child relationships between UNICLASS items
    * evolve_5 - Connects all areas creating a unique UNICLASS top level item
    * evolve_6 - Exposes the implicit dependency between every UNICLASS item and its corresponding 'rank'
    * evolve_7 - Exposes the implicit ranks hierarchy
    * evolve_8 - Completes the model exposing underlying high-level types and relationships, and connecting it to a top level ontology (BORO)

For each of the sub-stages, the user can check the progress of the analysis by visualising three kind of output files:

* Access databases
    * domain tables: collection of data tables showing a standard human-readable structure
    * nf ea com tables: collection of data tables which are the result of processing the domain tables to give them a more UML-friendly structure (influenced by the Enterprise Architect Data Model)
* [Enterprise Architect](https://sparxsystems.com/) model files

**This project is currently closed, but may be sporadically updated by the BORO Development Team in the future.**

## Installation

Ensure all the packages related in the file Requirements.txt are successfully installed.

This program has been tested with the [3.7.3 version of the Python interpreter](https://www.python.org/downloads/release/python-373/).

#### Requirements

The program needs some external resources to run:

* Microsoft ODBC Driver - Download from [here](https://www.microsoft.com/en-us/download/details.aspx?id=54920) and install
* Enterprise Architect with a valid (or trial) license - Download a trial version from [here](https://sparxsystems.com/products/ea/trial/request.html)
* In addition, Microsoft Access, or any other database viewer compatible with .accdb files

## Execution

* Run the startup file: **uniclass_to_nf_ea_com_source/a_startup/uniclass_bclearer_runner.py**
* This code runs as a standalone application that can be cloned to your Python IDE of preference
* This code only requires from the user the location of the parent folder where the output package will be exported

## Documentation

#### Background

The Agile Manifesto prefers “working software over comprehensive documentation”. Robert C. Martin, one of the original authors of the Agile Manifesto, is also the author of the book Clean Code.  

In this book, he makes a strong case for code being self-documenting: saying things such as "always try to explain yourself in code." 

He suggests that the goal of every programmer should be to write code so clean and expressive that code comments are unnecessary. 

When a programmer writes a comment, it will usually mean that they have failed to write code that was expressive enough. At the extreme, he suggests, maybe a little rhetorically, that "comments are always failures".

#### The BORO documentation policy

To aim to write code so clean and expressive that code comments are unnecessary. 

## Contributing

This package doesn't allow external contributors.

## Liability and Warranty

This code is provided as-is and without warranty.

Under no circumstances will the developers be liable for any incidental, consequential, or indirect damages including but not limited to lost or damaged data, revenue loss, economic loss, or commercial loss arising out of the use of this code.

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Acknowledgements

This work was developed as part of the Information Management Framework to support the [National Digital Twin programme](https://www.cdbb.cam.ac.uk/what-we-do/national-digital-twin-programme), and funded by [Department for Business, Energy & Industrial Strategy](https://www.gov.uk/government/organisations/department-for-business-energy-and-industrial-strategy) through the [Centre for the Protection of National Infrastructure](https://www.cpni.gov.uk/).
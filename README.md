# dept 
**Data engineering python toolkit (dept)** is a library of custom functions, function wrappers and scripts for common data engineering operations. Its intention is to remove boilerplate code from scripts to improve their readability, streamline common engineering tasks and preserve a collection of template scripts and blueprints.
    
**WHY?**  
Business never sleeps and adds new requirements every day. In such environment, saving time and mental capacity and preventing cognitive overload can make a difference, especially in the long run. Especially in business-facing data teams, fundamental enginneering tasks are recurring and generate a ton of boilerplate code which complicates code readability and understanding of the scripted processes.  
    
**dept** is attempting to create an abstraction layer over fundamental engineering operations and at the same time allow the code itself to represent a readable description of the operation/process. clear and undrestandable code can be easily reused and shared with other engineers without lengthy code walkthoughs and exlanations. 


## CONTENTS  
[Requirements](#requirements)  
[Setup](#setup)  
[Connectors](#connectors)  

## Requirements
- miniconda installation -> https://docs.anaconda.com/free/miniconda/index.html

## Setup
1. open miniconda terminal
2. navigate to your code repository folder
3. clone **dept** repository
  - clone dept as a standalone distribution  
  `git clone https://github.com/m-skp/dept.git`  
  - or navigate to your project repository and clone dept as a submodule for integration in your project  
  `git submodule add https://github.com/m-skp/dept.git`  
4. navigate to the cloned dept repository
5. install dept environment  
  `conda env create -f environment.yml`  
  `conda activate dept`  
6. *\[OPTIONAL\]* intall dept kernel for jupyter notebooks  
  `python -m ipykernel install --user --name dept`

## Connectors
- AWS
- Airflow
- PostgreSQL

\- to set up a connection, copy the .JSON connection config file template from [./configs/templates/](./configs/templates/) to [./configs/](./configs/) folder and populate the connection details  
\- **NOTE**: [./configs/](./configs/) folder is set up in `.gitingore` to prevent credentials to be commited and pushed outside the local repository


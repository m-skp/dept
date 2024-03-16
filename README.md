# dept 
**Data engineering python toolkit (dept)** is a library of custom functions, function wrappers and scripts for common data engineering operations. Its intention is to remove boilerplate code from scripts to improve their readability, streamline common engineering tasks and preserve a collection of template scripts and blueprints.
    
**WHY?**  
Data engineering niche is very diverse and quickly expanding. Business never sleeps and adds new requirements every day. In such hectic environment, saving time and mental capacity and can make a difference, especially in the long run. Some fundamental enginneering tasks are recurring and generate a ton of boilerplate code which complicates code readability and understanding of the scripted processes. Sharing code with other teams or team members requires clear and accessible documentation, code walkthoughs, lengthy exlanations or knowledge transfer sessions to be effective. Developers use different coding styles and approaches to achieve the same result. Some codes are pleasure to read, some just drain our brainpower.  
  
**dept** is attempting to create an abstraction layer over fundamental engineering operations and at the same time allow the code itself to represent a readable description of the operation/process. clear and undrestandable code helps save our mental capacity and focus on what matters.  

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
...
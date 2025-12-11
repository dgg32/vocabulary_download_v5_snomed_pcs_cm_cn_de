
# Introduction

  

This repository contains code and data for my article "[Building a Multilingual Medical Dictionary as a Graph Database](https://medium.com/@dgg32/graph-visualization-for-duckpgq-e6a551d11b94)".



# Prerequisite

Python

Account for athena.ohdsi.org

Neo4j Desktop


# Run

1. Download the Athena files from athena.ohdsi.org and the xlsx file from https://www.nhi.gov.tw/ch/cp-6071-469da-3051-1.html

2. Run clean.py to clean the Athena files.

3. Create and run a Neo4j database

4. Run the multilingual_import.py to import the cleaned data into Neo4j
```
python multilingual_import.py
```


## Authors



*  **Sixing Huang** - *Concept and Coding*

  

  

## License

  

  

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

# CodeGen

**This module is not reachable at runtime** (not included in docker image).
Do not reference it in any other module!

By running `main.py` you'll generate the SqlAlchemy tables, repositories, services and routers for all entities in DB.
The code can be found in the git-ignored local directory `./generated/`.

Use the IDE compare function to compare the generated code with the existing implementation. 

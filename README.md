# pyDQueue - dependent queing

A simple queuing system allowing to have dependencies between the tasks.

**This project is under current development and still experimental**


## Example
The following shows the queue of five tasks starting with a task called `Init`, followed 
by a task called `Task1` which takes the output from the task `Init`. The next task is named `C`
an expects the input from `Task1` but if this is not avaialble (e.g. because it crashed) it can 
start from task `Init` instead.

    Init() --> Task1(Init) --> C(Task1,Init) --> D(Init) --> E(D,Init)

A typical example for the above dependent queue layout might be a numerical simulation which expects 
input form a previous computation. However, if one crashes it can start from an alternative or some default 
initial conditions.
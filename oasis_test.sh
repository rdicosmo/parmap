#!/bin/bash

set -x

oasis setup
ocaml setup.ml -configure 
ocaml setup.ml -build
ocaml setup.ml -install

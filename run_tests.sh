#!/bin/bash

function run_tests {
    for stemmed in 'y' 'n'
    do
        for kernel_number in 3 2 1 0
        do
            ./reselect.py 'libsvm' $stemmed $kernel_number 95 "$1"
        done
    done

}


run_tests '../Processed Data/Fixtures for Users 0-99/Processed Data with Story Contents/'

;;;; -*- coding:utf-8 -*-

(in-package :cl-user)

(defpackage :columna
  (:use :cl)
  (:export
    ;;DB-OPs:
    #:create
    #:find
    #:update
    #:del
    )
  )

(in-package :columna)

(defparameter *dbs* (make-hash-table))

(defstruct db
  (name nil :type symbol)
  (tables nil :type list))

(defun create-db (name)
  (if (keywordp name)
    (unless (gethash name *dbs*)
      (setf (gethash name *dbs*) (make-db :name name :tables nil))) 
    (warn "Invalid name of type: ~A, must be a keyword" (type-of name))))

(defun create-table (name size db)
  (if (keywordp name)
    (if (and (> size 0)
             (integerp size))
      (unless (getf (db-tables db) name)
        (setf (getf (db-tables db) name)
              (make-array size)))
      (warn "Invalid size of table: ~A, must be a positive integer" size))
    (warn "Invalid name of type: ~A, must be a keyword" (type-of name))))

(defun create-col (name table)
  (if (keywordp name)
    (unless (member name table)
      (setf (getf table name) (list)))
    (warn "Invalid name of type: ~A, must be a keyword" (type-of name))))

(defun create (&key name type &optional size in)
  
  )

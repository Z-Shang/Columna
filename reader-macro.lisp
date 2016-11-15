;;;; -*- coding:utf-8 -*-

(in-package :columna)

(defvar *prev-readtable* nil)

(defun read-db-pos (stream char)
  (declare (ignore char))
  (let ((sym (read-delimited-list #\] stream)))
    (case (length sym)
      (0 (warn "Invalid DB Pos"))
      (1 `(get-pos :DB ',(first sym)))
      (2 `(get-pos :DB ',(first sym) :TABLE ',(second sym)))
      (3 `(get-pos :DB ',(first sym) :TABLE ',(second sym) :COL ',(third sym)))
      (4 `(get-pos :DB ',(first sym) :TABLE ',(second sym) :COL ',(third sym) :POS ,(fourth sym)))
      (t (warn "DB Position Too Long")))))

(defun read-selector (stream char)
  (declare (ignore char))
  (let* ((col (read stream nil nil nil))
         (pred (read stream nil nil nil)))
    `(make-selector :pred ,pred :col ',col)))

(defmacro enable-reader-macro ()
  '(eval-when (:compile-toplevel :load-toplevel :execute)
    (push *readtable* *prev-readtable*)
    (setq *readtable* (copy-readtable))
    (set-macro-character #\$ #'read-selector)
    (set-macro-character #\[ #'read-db-pos)
    (set-macro-character #\] (get-macro-character #\)))))

(defmacro disable-reader-macro ()
  '(eval-when (:compile-toplevel :load-toplevel :execute)
    (when *prev-readtable*
      (setq *readtable* (pop *prev-readtable*)))))

(push *readtable* *prev-readtable*)
(setq *readtable* (copy-readtable))
(set-macro-character #\[ #'columna:read-db-pos)
(set-macro-character #\] (get-macro-character #\)))

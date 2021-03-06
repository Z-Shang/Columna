;;;; -*- coding:utf-8 -*-

(in-package :cl-user)

(defpackage :columna
  (:use :cl)
  (:export
   ;;DB-OPs:
   #:set-db-path
   #:create-db
   #:create-table
   #:create-col
   #:create-with-schema
   #:insert
   #:lookup
   #:update
   #:del
   #:get-pos
   #:take
   #:mapcol

   ;;File System IO
   #:write-data-to-file
   #:load-data-from-file
   #:load-data-from-file-to-db
   #:with-db-file

   ;;Reader-Macro:
   #:read-db-pos
   #:enable-reader-macro
   #:disable-reader-macro

   ;;Utils:
   #:make-selector
   #:_v
   #:_i))

(in-package :columna)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload :bordeaux-threads))

(defparameter *dbs* (make-hash-table))
(defparameter *lock* (bt:make-recursive-lock))
(defparameter *default-db-path* #P"~/.columna/")

(defun set-db-path (path)
  (setf *default-db-path* path))

(defmacro with-lock (&body body)
  `(bt:with-recursive-lock-held (*lock*)
     ,@body))

(defun first-nil (arr)
  (loop :for i :from 0 :to (1- (length arr))
        :when (null (aref arr i))
          :return i
        :finally (return -1)))

(defun lookup-table (name table)
  (if (listp name)
      (mapcar #'(lambda (n) (list 'quote (lookup-table n table))) name)
      (loop :for i :from 0 :to (1- (length table))
            :when (equal name (getf (aref table i) :name))
              :return (getf (aref table i) :data)
            :finally (return 'DNE))))

(defstruct db
  (name nil :type symbol)
  (tables nil :type list))

(defstruct selector
  (pred nil :type function)
  (col nil :type (or symbol list)))

(defun create-db (name)
  (with-lock
    (if (symbolp name)
        (unless (gethash name *dbs*)
          (setf (gethash name *dbs*) (make-db :name name :tables nil)))
        (warn "Invalid name of type: ~A, must be a symbol" (type-of name)))))

(defun create-table (name size db)
  (with-lock
    (if (symbolp name)
        (if (and (> size 0)
                 (integerp size))
            (unless (getf (db-tables db) name)
              (setf (getf (db-tables db) name)
                    (make-array size :initial-element nil)))
            (warn "Invalid size of table: ~A, must be a positive integer" size))
        (warn "Invalid name of type: ~A, must be a symbol" (type-of name)))))

(defun create-col (name table &optional &key pred)
  (with-lock
    (if (symbolp name)
        (unless (< (first-nil table) 0)
          (setf (aref table (first-nil table))
                (list :name name
                      :data (list)
                      :pred (if pred
                                pred
                                #'(lambda (x)
                                    (declare (ignore x))
                                    t)))))
        (warn "Invalid name of type: ~A, must be a symbol" (type-of name)))))

(defun insert (values table)
  (cond
    ((listp values)
     (with-lock
       (if (= (length values) (length table))
           (let ((types
                   (loop :for i :from 0 :to (1- (length table))
                         :collect (funcall (getf (aref table i) :pred) (nth i values)))))
             (if (loop :for i :from 0 :to (1- (length types))
                       :when (not (nth i types))
                         :do (warn "Type: ~A of values at: ~A doesn't match the predicate of column" (type-of (nth i values)) i)
                       :always (nth i types))
                 (loop :for i :from 0 :to (1- (length table))
                       :do (setf (getf (aref table i) :data) (cons (nth i values)
                                                                   (getf (aref table i) :data))))))
           (warn "Values doesn't match table size: ~A" (length table)))))))

(defun get-pos (&key db (table nil) (col nil) (pos nil))
  (let ((d (gethash db *dbs*)))
    (if d
        (if table
            (let ((tbl (getf (db-tables d) table)))
              (if tbl
                  (if col
                      (let ((c (lookup-table col tbl)))
                        (if (listp c)
                            (if pos
                                (if (integerp pos)
                                    (if (< pos (length c))
                                        (nth pos c)
                                        (warn "Position: ~A is out of range" pos))
                                    (warn "Invalid position: ~A" pos))
                                c)
                            (warn "Column: ~A doesn't exist in ~A.~A" col db table)))
                      tbl)
                  (warn "Table: ~A doesn't exist in DB: ~A" table db)))
            d)
        (warn "DB: ~A doesn't exist" db))))

(defun mapcam (f n lst)
  (if (null lst)
      nil
      (if (not (zerop (mod (length lst) n)))
          (error "Then length of list: ~A is not a multiple of number: ~A" (length lst) n)
          (cons (apply f (subseq lst 0 n))
                (mapcam f n (nthcdr n lst))))))

(defmacro create-with-schema (db &body body)
  `(if (oddp (length ',body))
       (warn "Invalid number of arguments: ~A" (length ',body))
       (progn
         (create-db ',db)
         (mapcam #'(lambda (tbl cols)
                     (create-table tbl
                                   (length cols)
                                   (get-pos :db ',db))
                     (mapcar #'(lambda (c)
                                 (if (consp c)
                                     (create-col (car c)
                                                 (get-pos :db ',db :table tbl)
                                                 :pred (eval (cadr c)))
                                     (create-col c
                                                 (get-pos :db ',db :table tbl))))
                             cols))
                 2
                 ',body))))

(defun del (p table)
  (typecase p
    (integer
     (with-lock
       (let ((l (1- (length table))))
         (loop :for i :from 0 :to l
               :do (setf (getf (aref table i) :data)
                         (remove-if (constantly t) (getf (aref table i) :data)
                                    :start p :count 1))))))
    (selector
     (let ((pivot (lookup-table (selector-col p)
                                table)))
       (if (equal pivot 'DNE)
           (error "Invalid selector with pivot column: ~A" (selector-col p))
           (with-lock
             (let* ((l (1- (length pivot)))
                    (r
                      (loop :for i :from 0 :to l
                            :when (not (funcall (selector-pred p) (nth i pivot)))
                              :collect (loop :for j :from 0 :to (1- (length table))
                                             :collect (nth i (getf (aref table j) :data)))))
                    (res (loop :for i :from 0 :to (1- (length (car r)))
                               :collect (mapcar #'(lambda (l) (nth i l)) r))))
               (loop :for i :from 0 :to (1- (length table))
                     :do (setf (getf (aref table i) :data)
                               (nth i res))))))))))

(defun lookup (p table)
  (typecase p
    (list
     (if (loop :for i :in p :thereis (not (integerp i)))
         (error "Invalid argument, [integer] or selector only")
         (with-lock
           (loop :for i :in p
                 :collect (loop :for j :from 0 :to (1- (length table))
                                :collect (nth i
                                              (getf (aref table j) :data)))))))
    (selector
     (if (listp (selector-col p))
         (let ((pivots (lookup-table (selector-col p) table)))
           (if (loop :for piv :in pivots :thereis (equalp piv 'DNE))
               (error "Invalid selector with pivot columns: ~A" (selector-col p))
               (if (> (length (remove-duplicates (mapcar #'length pivots))) 1)
                   (error "The pivot columns' lengths don't match")
                   (let ((marks (eval `(mapcar ,(selector-pred p) ,@pivots))))
                     (with-lock
                       (loop :for i :from 0 :to (1- (length marks))
                             :when (nth i marks)
                               :collect (loop :for j :from 0 :to (1- (length table))
                                              :collect (nth i (getf (aref table j) :data)))))))))
         (let ((pivot (lookup-table (selector-col p) table)))
           (if (equal pivot 'DNE)
               (error "Invalid selector with pivot column: ~A" (selector-col p))
               (with-lock
                 (loop :for i :from 0 :to (1- (length pivot))
                       :when (funcall (selector-pred p) (nth i pivot))
                         :collect (loop :for j :from 0 :to (1- (length table))
                                        :collect (nth i (getf (aref table j) :data)))))))))))

;;N could be a function that takes the index and the current value
(defun update (p n table)
  (when (listp n)
    (if (not (= (length n) (length table)))
        (error "Invalid number of arguments, need: ~A, given: ~A" (length table) (length n))))
  (typecase p
    (list
     (if (loop :for i :in p :thereis (not (integerp i)))
         (error "Invalid argument, [integer] or selector only")
         (with-lock
           (loop :for i :in p
                 :do (loop :for j :from 0 :to (1- (length table))
                           :do
                              (if (listp n)
                                  (if (functionp (nth j n))
                                      (setf (nth i (getf (aref table j) :data))
                                            (funcall (nth j n) i (nth i (getf (aref table j) :data))))
                                      (setf (nth i (getf (aref table j) :data)) (nth j n)))
                                  (if (functionp n)
                                      (setf (nth i (getf (aref table j) :data))
                                            (funcall n i (nth i (getf (aref table j) :data))))
                                      (setf (nth i (getf (aref table j) :data)) n))))))))
    (selector
     (if (listp (selector-col p))
         (let ((pivots (lookup-table (selector-col p) table)))
           (if (loop :for piv :in pivots :thereis (equalp piv 'DNE))
               (error "Invalid argument, [integer] or selector only")
               (if (> (length (remove-duplicates (mapcar #'length pivots))) 1)
                   (error "The pivot columns' lengths don't match")
                   (let ((marks (eval `(mapcar ,(selector-pred p) ,@pivots))))
                     (with-lock
                       (loop :for i :from 0 :to (1- (length marks))
                             :when (nth i marks)
                               :do (loop :for j :from 0 :to (1- (length table))
                                         :do
                                            (if (listp n)
                                                (if (functionp (nth j n))
                                                    (setf (nth i (getf (aref table j) :data))
                                                          (funcall (nth j n) i (nth i (getf (aref table j) :data))))
                                                    (setf (nth i (getf (aref table j) :data)) (nth j n)))
                                                (if (functionp n)
                                                    (setf (nth i (getf (aref table j) :data))
                                                          (funcall n i (nth i (getf (aref table j) :data))))
                                                    (setf (nth i (getf (aref table j) :data)) n))))))))))
         (let ((pivot (lookup-table (selector-col p) table)))
           (if (equal pivot 'DNE)
               (error "Invalid argument, [integer] or selector only")
               (with-lock
                 (loop :for i :from 0 :to (1- (length pivot))
                       :when (funcall (selector-pred p) (nth i pivot))
                         :do (loop :for j :from 0 :to (1- (length table))
                                   :do
                                      (if (listp n)
                                          (if (functionp (nth j n))
                                              (setf (nth i (getf (aref table j) :data))
                                                    (funcall (nth j n) i (nth i (getf (aref table j) :data))))
                                              (setf (nth i (getf (aref table j) :data)) (nth j n)))
                                          (if (functionp n)
                                              (setf (nth i (getf (aref table j) :data))
                                                    (funcall n i (nth i (getf (aref table j) :data))))
                                              (setf (nth i (getf (aref table j) :data)) n))))))))))))

(defun take (n table)
  (when (not (integerp n))
    (error "~A is not an integer!" n))
  (lookup (loop :for n :from 0 :to (1- n) :collect n) table))

;; Map a function over a table
(defun mapcol (fn tbl)
  (with-lock
    (let ((table (loop :for p :from 0 :to (1- (length tbl)) :collect (cons 'list (getf (aref tbl p) :data)))))
      (eval `(mapcar ,fn ,@table)))))

(defun _v (i v)
  (declare (ignore i))
  v)

(defun _i (i v)
  (declare (ignore v))
  i)

(defun write-data-to-file (db-name file-path)
  (let ((db (gethash db-name *dbs*)))
    (if (null db)
        (error "DB: ~A Doesn't exist!" db-name)
        (with-lock
          (progn
            (ensure-directories-exist file-path)
            (with-open-file (file (merge-pathnames (make-pathname :name (write-to-string db-name)) file-path)
                                  :direction :output
                                  :if-exists :supersede
                                  :if-does-not-exist :create)
              (let ((tbls (mapcam #'(lambda (x y)  (cons x y)) 2 (db-tables db))))
                (loop for tbl in tbls do
                  (format file "~A~%" (write-to-string (list :n (car tbl)
                                                             :v (loop for c being the elements of (cdr tbl)
                                                                      collect (getf c :data)))))))))))))

(defun load-data-from-file-to-db (file-path db &key (override nil))
  (with-open-file (file file-path :direction :input :if-does-not-exist nil)
    (if (null file)
        (error "DB File: ~A Doesn't exist!" file-path)
        (with-lock
          (when file
            (loop :for line = (read-line file nil)
                  :while line
                  :do (let* ((data (read-from-string line))
                             (tbl-name (getf data :n))
                             (values (getf data :v))
                             (tbl (getf (db-tables db) tbl-name)))
                        (loop :for i :from 0 :to (1- (length values))
                              :do (if override
                                      (setf (getf (aref tbl i) :data)
                                            (nth i values))
                                      (setf (getf (aref tbl i) :data)
                                            (append (nth i values) (getf (aref tbl i) :data))))))))))))

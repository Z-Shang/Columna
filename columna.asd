(in-package :cl-user)

(defpackage :columna-asd
  (:use :asdf :cl))

(in-package :columna-asd)

(defsystem :columna
  :name "Columna"
  :description "A lightweight columnar database"
  :version "0.0.1"
  :author "Z.Shang <shangzhanlin@gmail.com>"
  :license "GPL3"
  :components ((:file "columna")
               (:file "reader-macro" :depends-on ("columna"))))

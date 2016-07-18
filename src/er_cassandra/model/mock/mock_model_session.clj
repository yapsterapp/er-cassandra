(ns er-cassandra.model.mock.mock-model-session)

(defprotocol MockModelSession
  (-check [this]))

(defprotocol MockModelSpySession
  (-mock-model-spy-log [this]))

(defprotocol Matcher
  (-match [this request])
  (-finish [this]))

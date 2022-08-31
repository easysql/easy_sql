package your.company

import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction

class TestFunction extends ScalarFunction {
  def eval(a: Integer, b: Integer): Integer = {
    a + b + 10
  }
}

object udfs {
    def initUdfs(flink: TableEnvironment) {
        flink.createTemporarySystemFunction("test_func", classOf[TestFunction])
    }
}
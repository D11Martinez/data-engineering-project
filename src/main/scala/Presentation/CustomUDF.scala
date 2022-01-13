package presentation

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CustomUDF {
  val getLastUDF: UserDefinedFunction = udf((xs: Seq[String]) => xs.last)

  val getFilePathUDF: UserDefinedFunction =
    udf((elements: Seq[String]) => {
      elements.size match {
        case 0 => ""
        case 1 => ""
        case _ => elements.slice(0, elements.size - 1).mkString("/")
      }
    })

  val getLanguageFromExtension: UserDefinedFunction = udf((extension: String) => {
    val javaExtensions = Array("java", "class", "jar", "jmod", "jsp")
    val javaScriptExtensions = Array("js", "cjs", "mjs")
    val typeScriptExtensions = Array("ts", "tsx")
    val phpExtensions = Array("php", "phar", "phtml", "pht", "phps")
    val pythonExtensions = Array("py", "pyi", "pyc", "pyd", "pyo", "pyw", "pyz")
    val scalaExtensions = Array("scala", "sc")
    val cExtensions = Array("c", "cc", "cpp", "cxx", "c++", "h", "H", "hh", "hpp", "hxx", "h++")
    val rExtensions = Array("r", "rdata", "rds", "rda")
    val goExtensions = Array("go")
    val rubyExtensions = Array("rb")
    val dartExtensions = Array("dart")
    val cCharExtensions = Array("cs", "csx")
    val kotlinExtensions = Array("kt", "kts", "ktm")
    val swiftExtensions = Array("swift", "SWIFT")
    val rustExtensions = Array("rs", "rlib")
    val luaExtensions = Array("lua")
    val sqlExtensions = Array("sql")

    extension match {
      case x if typeScriptExtensions.contains(x) => "TypeScript"
      case x if javaScriptExtensions.contains(x) => "JavaScript"
      case x if pythonExtensions.contains(x)     => "Python"
      case x if javaExtensions.contains(x)       => "Java"
      case x if scalaExtensions.contains(x)      => "Scala"
      case x if cExtensions.contains(x)          => "C/C++"
      case x if rExtensions.contains(x)          => "R"
      case x if phpExtensions.contains(x)        => "PHP"
      case x if cCharExtensions.contains(x)      => "C#"
      case x if kotlinExtensions.contains(x)     => "Kotlin"
      case x if swiftExtensions.contains(x)      => "Swift"
      case x if goExtensions.contains(x)         => "Go"
      case x if dartExtensions.contains(x)       => "Dart"
      case x if rubyExtensions.contains(x)       => "Ruby"
      case x if rustExtensions.contains(x)       => "Rust"
      case x if luaExtensions.contains(x)        => "Lua"
      case x if sqlExtensions.contains(x)        => "SQL"
      case _                                     => "Other"
    }
  })

  val validateMessageUDF: UserDefinedFunction =
    udf((message: String) => {
      if (message.nonEmpty) {
        val isGoodLength = message.length <= 80

        isGoodLength
      } else false
    })

  val getIntervalCategory: UserDefinedFunction = udf((quantityStr: String) => {
    quantityStr.toInt match {
      case x if 0 until 16 contains x       => "0 - 15"
      case x if 16 until 51 contains x      => "16 - 50"
      case x if 51 until 101 contains x     => "51 - 100"
      case x if 101 until 501 contains x    => "101 - 500"
      case x if 501 until 1001 contains x   => "501 - 1000"
      case x if 1001 until 2501 contains x  => "1001 - 2500"
      case x if 2501 until 5001 contains x  => "2501 - 5000"
      case x if 5001 until 10001 contains x => "5001 - 10000"
      case x if x > 10000                   => "10000+"
      case _                                => "Uncategorized"
    }
  })
}

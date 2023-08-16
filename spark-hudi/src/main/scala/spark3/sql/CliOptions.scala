package spark3.sql

import com.beust.jcommander.Parameter
import java.io.Serializable

class CliOptions extends Serializable {
  @Parameter(names = Array(Array("-q", "--queries")), description = "sql query names. If the value is 'all', all queries will be executed.", required = true) var queries: String = null
  @Parameter(names = Array(Array("-d", "--database")), description = "sql query names. If the value is 'all', all queries will be executed.", required = true) var database: String = null
}
package com.kish.learning

import com.apurebase.kgraphql.*
import io.ktor.application.*
import io.ktor.auth.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.h2.store.Page.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.*
import kotlin.math.*

data class Employee(var id:Long, val fname:String, val lname:String , val age:Int,val email:String)
data class GraphQLRequest(val query:String)

object Employees: Table(){

	val id: Column<Long> = long("id").autoIncrement()
	val fname: Column<String> = varchar("fname",255)
	val lname: Column<String> = varchar("lname",255)
	val email: Column<String> = varchar("email",255)
	val age: Column<Int> = integer("age")

	override val primaryKey= PrimaryKey(id,name = "pk_employee")

	fun toEmployee(row: ResultRow): Employee=
		Employee(
			fname = row[Employees.fname],
			lname = row[Employees.lname],
			age = row[Employees.age],
			email = row[Employees.email],
			id = row[Employees.id]
		)

}



fun Application.employeeApp(){

	// Database in file, needs full path or relative path starting with ./
//    Database.connect("jdbc:h2:./myh2file", "org.h2.Driver")
// In memory
//    Database.connect("jdbc:h2:mem:regular", "org.h2.Driver")
// In memory / keep alive between connections/transactions

	Database.connect("jdbc:h2:mem:regular;DB_CLOSE_DELAY=-1;", "org.h2.Driver")

	transaction {
		SchemaUtils.create(Employees)

		Employees.insert {
			 it[fname] = "Thej"
			it[lname] = "Karuneegar"
			it[age] = 36
			it[email] = "kishores@gmail.com"
		}

		Employees.insert {
			it[Employees.fname] = "Shanaya"
			it[lname] = "Karuneegar"
			it[age] = 3
			it[email] = "shanaya@gmail.com"
		}

		Employees.insert {
			it[fname] = "Bharathi"
			it[lname] = "Karuneegar"
			it[age] = 56
			it[email] = "bharathi@gmail.com"
		}


		Employees.insert {
			it[fname] = "Ganes"
			it[lname] = "Karuneegar"
			it[age] = 66
			it[email] = "ganesh@gmail.com"
		}

		Employees.insert {
			it[fname] = "Abirami"
			it[lname] = "Balasubramanian"
			it[age] = 29
			it[email] = "abi@gmail.com"
		}

		Employees.insert {
			it[fname] = "Arun Kumar"
			it[lname] = "Balasubramanian"
			it[age] = 32
			it[email] = "arun@gmail.com"
		}
	}

	// Initializing the Graph ql

	val schema = KGraphQL.schema {
			query("persons"){
				resolver { ->
					transaction{
						Employees.selectAll().map { Employees.toEmployee(it) }
					}
				}
			}

			query("person"){
				resolver { id:Long ->
					transaction {
						Employees.select { Employees.id eq id }.map { Employees.toEmployee(it) }
					}

				}
			}

		mutation("updatePerson"){
			resolver { id:Long,age:Int ->
				transaction {
					Employees.update ({Employees.id eq id}) {
							it[Employees.age] = age
					}
				}

			}
		}
	}

	routing {

		route("graphql"){
			get("/"){
				val graphQlReq = call.receive<GraphQLRequest>()
				call.respond(schema.execute(graphQlReq.query))
			}
		}

		authenticate ("myBasicAuth") {
			route("/employee") {
				get("/") {
					val users = transaction {
						Employees.selectAll().map { Employees.toEmployee(it) }
					}
					call.respond(users)
				}

				get("/{id}"){
					val id= call.parameters["id"]!!.toLong()
					val resultRow = transaction {
								Employees.select { Employees.id eq id }.map { Employees.toEmployee(it) }
					}
					call.respond(resultRow)
				}

				post("/"){
					val inputEmployee = call.receive<Employee>()
					val insertEmployee = transaction {
						Employees.insert{
							it[Employees.fname] = inputEmployee.fname
							it[Employees.lname] = inputEmployee.lname
							it[Employees.age] = inputEmployee.age
							it[Employees.email] = inputEmployee.email
						}
					}
					Employees.toEmployee(insertEmployee.resultedValues!!.get(0))
					call.respond(Employees.toEmployee(insertEmployee.resultedValues!!.get(0)))
				}
			}
		}
	}



}
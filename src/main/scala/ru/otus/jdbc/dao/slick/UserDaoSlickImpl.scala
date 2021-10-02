package ru.otus.jdbc.dao.slick



import ru.otus.jdbc.model.{Role, User}
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}




class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {
  import UserDaoSlickImpl._

  private def userExists(userId: UUID): Future[Boolean] = {
    db.run(users.filter(_.id === userId).exists.result)
  }

  def getUser(userId: UUID): Future[Option[User]] = {
    val res = for {
      user  <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }

  def createUser(user: User): Future[User] = {
    val userRow = UserRow.fromUser(user)
    userExists(user.id).flatMap {
      case true =>
        Future.successful(user)
      case false =>
        val res = DBIO.seq(
          users += userRow,
          usersToRoles ++= user.roles.map(user.id -> _)
        ).transactionally

        db.run(res.map(_ => user))
    }
  }

  def updateUser(user: User): Future[Unit] = {
    val updateUser = users
      .filter(_.id === user.id)
      .map(u => (u.firstName, u.lastName, u.age))
      .update((user.firstName, user.lastName, user.age))

    val deleteRoles = usersToRoles.filter(_.usersId === user.id).delete
    val insertRoles = usersToRoles ++= user.roles.map(user.id -> _)

    val action =
      DBIO.seq(
        updateUser,
        deleteRoles,
        insertRoles,
      ).transactionally.withTransactionIsolation(TransactionIsolation.Serializable)

    userExists(user.id).flatMap {
      case true =>
        db.run(action)
      case false =>
        createUser(user).map(_ => ())
    }
  }

  def deleteUser(userId: UUID): Future[Option[User]] =
    getUser(userId).flatMap {
      case Some(user) =>
        val res = DBIO.seq(
          usersToRoles.filter(_.usersId === userId).delete,
          users.filter(_.id === userId).delete
        ).transactionally

        db.run(res.map(_ => Some(user)))

      case None =>
        Future.successful(None)
    }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] = {
    val query = users
      .filter(condition)
      .joinLeft(usersToRoles)
      .on(_.id === _.usersId)
      .map {
        case (user, role) => (user, role.map(_.rolesCode))
      }
      .result

    val res = query
      .map(_.groupBy { case (user, _) => user })
      .map(_.map {
        case (userRow, value) => userRow.toUser(value.filter(_._2.isDefined).map(_._2.get).toSet)
      }.toVector)

    db.run(res)
  }

  def findByLastName(lastName: String): Future[Seq[User]] =
    findByCondition(_.lastName === lastName)

  def findAll(): Future[Seq[User]] =
    findByCondition(_ => true)

  private[jdbc] def deleteAll(): Future[Unit] =
    db.run {
      DBIO.seq(
        usersToRoles.delete,
        users.delete
      ).transactionally
    }
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
        case "reader"  => Role.Reader
        case "manager" => Role.Manager
        case "admin"   => Role.Admin
    }
  )


  case class UserRow(
      id: UUID,
      firstName: String,
      lastName: String,
      age: Int
  ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((UUID, String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id        = column[UUID]("id", O.PrimaryKey)
    val firstName = column[String]("first_name")
    val lastName  = column[String]("last_name")
    val age       = column[Int]("age")

    val * = (id, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId   = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}

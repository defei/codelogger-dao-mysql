package org.codelogger.dao.test.domain;

import org.codelogger.dao.stereotype.Entity;
import org.codelogger.dao.stereotype.Id;

@Entity(tableName = "user")
public class User {

  @Id
  private Long id;

  private String name;

  private String email;

  @Override
  public String toString() {

    return "User [id=" + id + ", name=" + name + ", email=" + email + "]";
  }

  public Long getId() {

    return id;
  }

  public void setId(final Long id) {

    this.id = id;
  }

  public String getName() {

    return name;
  }

  public void setName(final String name) {

    this.name = name;
  }

  public String getEmail() {

    return email;
  }

  public void setEmail(final String email) {

    this.email = email;
  }

}

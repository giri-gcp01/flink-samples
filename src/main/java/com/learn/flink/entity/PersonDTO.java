package com.learn.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class PersonDTO {

    String id;
    String userId;
    String firstName;
    String lastName;
    String gender;
    String email;
    String phone;
    String dateOfBirth;
    String jobTitle;


    @Override
    public java.lang.String toString() {
        return new StringBuilder().append(this.getId()).append(",")
                .append(this.getUserId()).append(",")
                .append(this.getFirstName()).append(",")
                .append(this.getLastName()).append(",")
                .append(this.getGender()).append(",")
                .append(this.getEmail()).append(",")
                .append(this.getPhone()).append(",")
                .append(this.getDateOfBirth()).append(",")
                .append(this.getJobTitle()).toString();
    }
}


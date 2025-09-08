CREATE Table User(
    user_ID int NOT NULL primary key,
    first_name Varchar(50),
    last_name Varchar(50),
    phone_number int (100),
    gender Varchar (10),
    interested_in Varchar (10),
    age int CHECK (date_of_birth >= 18),
    acc_creation_date date NOT NULL,
    acc_last_update date,
    foto text,
    street Varchar (50),
    zip int (5),
    city Varchar (50)
)

    
CREATE Table Hobby(
    hobby_ID int NOT NULL primary key,
    hobbyname Varchar (50) NOT NULL
)


CREATE Table user_hobby(
    user_hobby_ID int NOT NULL primary key,
    foreign key (user_ID) references User(user_ID),
    foreign key (hobby_ID) references User(hobby_ID)
    prio int (3)
)

CREATE Table Friendship(
    frindship_ID primary key,
    user_ID NOT NULL int,
    foreign key (user_ID) references User(user_ID),
    friend_unser_ID NOT NULL int,
    foreign key (friend_user_ID) references User(user_ID)
)

CREATE Table Like(
    like_ID int NOT NULL primary key,
    foreign key (user_ID) references User(user_ID),
    foreign key (liked_user_ID) references User(user_ID),
    status int (1),
    timestamp datetime
)

CREATE Table Message(
    message_ID int NOT NULL primary key,
    conversation_ID int NOT NULL,
    foreign key (sender_user_ID) references User(user_ID),
    foreign key (reciever_user_ID) references User(user_ID),
    message_body text,
    timestamp datetime
)

CREATE Table Friendship_Message(
    friendship_message_ID int NOT NULL primary key,
    foreign key (friendship_ID) references (user_ID)
    foreign key (message_ID) references (user_ID)
    prio int (3)
    timestamp datetime
)

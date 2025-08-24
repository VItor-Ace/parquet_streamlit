# Parquet Streamlit Web App
Here is a repository built for integration with Streamlit Community Cloud platform. It consists of a '.gitignore' file, preserving some sensitive information which could be available
publicly, a 'requirements' file, indicating which libraries and versions to use for better performance, and the Python file, cointaining the code wrote using the streamlit library
for implementation and integration.

## Authentication
Since I was planning to use the Community Cloud service, to avoid costs using the Snowflake's service for Streamlit, I would face a complication due to the fact that these sensitive
information about the clients were going to be public. To prevent any risks, it was added an authentication step using the Streamlit Cloud's feature 'Streamlit Secrets'. It is a 
setting which can only be viewed and modified by the administrator of the web application. 

I highly recommend using the Streamlit's feature Secrets, so you can refer to some variables which must be confidential and have to be used on your Python code, without literally
expressing them, turning public. This feature was used to store the Parquet's file local in AWS S3 storage, such as the access key, password and its bucket on S3 platform. 
Additionally, for authentication, the Secrets were used to store the user's credentials: username, encoded password and e-mail.

Nevertheless, it comes with some limitations. Differently from the payed Streamlit Cloud service, these credentials cannot be modified inside the application. Thus, some actions
such as 'Modify password', 'Modify username', etc. cannot be implement directly. They are subject to a manual modification on Streamlit Secrets settings. If your web application is 
not aimed at a big staff with a great number of users, then I do not think it will be a great obstacle.

For more and complete information about Authentication on Streamlit, I recommend this repository - which I used: https://github.com/mkhorasani/Streamlit-Authenticator

## Parquet Table Visualization and Editing
Since Streamlit is already a complete platform and has full commands, I used some functions and commands directly to turn editing available for the user. By default, the application
opens the Parquet file saved on S3, allowing its visualization and edition. My client complained about the insecurity and ease of deleting one row on an Excel spreadsheet accidentally.
To handle it on Parquet, I added a two-step verification for saving changes on the table if a line is removed.

Creating a function to generate a random three digits verification code, the user can only save the changes if he/she enters the code correctly. Otherwise, the modifications will 
not be saved and the table will remain unchanged. In this way, I can fix all of my client's needs.


# Conclusion
This web application was a great opportunity for me to work with front-end programming. Also, it was a good example of how a desire, even if it gives us more hard work, can be done
and pursued to attend the demands of the client, that was the case when I was searching a database file to replace the Excel spreadsheet and I found out that a Parquet file is not
human readable and using it would require more tools for visualization and edition.

Additionally, it was fantastic to know the Streamlit platform. Which impressed me, I am not going to lie! I found its design very good without a bunch of code lines to do it.

# Safebooru-React-Component

This component is the react-component which is used to provide a frontend where the data that was gathered and analyzed
is displayed.

In order to execute the react app node 14.17.1 (or higher) and npm 6.14.15 (or higher) are used.

## Installing the required modules

In order to execute the react-app, you first need to install the required modules. This can be done via npm (node
package manager). In order to install the modules the following command needs to be executed within this directory

`npm install`

After this command is executed you need to confirm that you want to download all the modules. Thereafter, the download
is started, and you are notified once it is finished. After that you can execute the code.

### Execution Method 1 (npm start)

This project can be started by executing the command `npm start` within this directory. This causes the app to be
started from the console. The app can be stopped by either closing the console or by stopping the execution within the
console.

### Execution Method 2 (npm build + pm2)

Another way to start the project is by first building a folder and the use pm2 (project manager 2) in order to serve it.
In order to use this method pm2 needs to be installed. This can be done via the following command:

`npm install pm2`

After that the following command should be executed whiting this directory in order to create a folder that can be
served.

`npm build`

After this command was executed a folder named "build" is created. This folder can now be deployed by using pm2. In
order to deploy this folder the following command needs to be executed:

` pm2 serve build <port> --spa`

This command causes pm2 to start serving the build folder. The <port> argument can be used to define a port on which the
component can be reached. After this command was executed the folder is now served via pm2 and is also restarted if a
crash of the app should occur.


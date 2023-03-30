# smoke
Hamish Gordon's group space for development and deployment of C3 AI DTI applications.

### Main components
The `smoke` space in C3's IDS, which lives in DTI RC01, currently has one application (`smokeApp`) and one branch (`dev/smokeApp/rc1`). The latter corresponds to a C3 tag that can be access via the vanity URL [https://dev01-smokeapp.devrc01.c3aids.cloud/static/console/](https://devsmokeapprc1-smokeapp.devrc01.c3aids.cloud/static/console/)

### Typical development workflow
This GitHub repository always contains at least three branches: 

- an intrinsic `main` branch that hosts the application code for each released version.
- a C3 `develop` branch that is connected to C3 AI Studio (formerly IDS)
- `dev` branches that represents what is deployed to the tag above

The typical development workflow then goes as follows:

1. Clone the repository to your local environment (we highly encourage using VS Code)
2. Create a new branch **from** `dev/smokeApp/rc1` and give it a meaningful name, e.g. `username-dev`
3. Develop code, regularly commit, and push back to GitHub when you want the code provisioned to the tag
4. Open a pull request **from** `username-dev` **into** `dev/smokeApp/rc1`
5. Github Actions that DTI DevOps have running will then trigger the provisioning of the package to the `dev/smokeApp/rc1` tag (URL above)

As long as the pull request is open, pushing code to GitHub will always trigger the provisioning workflow. 
For that reason, if you have multiple developers in your team, we recommend that you consider more ephemeral pull requests to tackle simple tasks at a time. You are welcome to use GitHub Projects to give them tags and other details, as well as using GitHub Issues to describe the steps of more complex development efforts that can then be broken into several small tasks, each one with its own PR (open, close, next). 

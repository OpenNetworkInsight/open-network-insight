# **Open Network Insight**   <sup><sup><sub>_The business of network security - the "port perspective"_</sub></sup></sup>

Open Network Insight is open source software for leveraging insights from flow and packet analysis. It helps enterprises and service providers gain insight on their compute environments through transparency of service delivery and identification of potential security threats or attacks happening among resources operating at cloud scale.

While current threat intelligence tools help, identifying unknown threats and attacks remains a challenge. Open Network Insight provides tools to accelerate companies’ ability to expose suspicious connections and previously unseen attacks using flow and packet analysis technologies. 
<br><br>

![oni logo](docs/oni-guy.png "ONI")![](docs/oni-logo.png "ONI")

----

## **Overview**
![oni approach](docs/oni-approach.png "ONI Approach")

With the arrival of big data platforms, security organizations can now make data-driven decisions about how they protect their assets.  Records of network traffic, captured as network flows, are often stored and analyzed for use in network management.  An organization can use this same information to gain insight into what channels corporate information flows through. 

By taking into account additional context such as prevalent attacks and key protocols to the organization, the security team can develop a strategy that applies the right amount of per-channel risk mitigation based on the value of the data flowing through it.  For an organization, we call this “the port perspective”. 

There are two vectors that all organizations should evaluate:

 * A “wide enough, deep enough” protection strategy that involves both edge prevention and sophisticated detection of unusual behavior

 * A deep inspection of key protocols using methods that can scale to the volume of data flowing across that channel

While inspecting specific, unique flows of data that may be important for individual organizations, all organizations can realize significant risk reduction from analysis of network flows and DNS (domain name service) replies.

**Open Network Insight**  by leveraging strong technology in both Big Data and Scientific Computing disciplines is a solution intended to support this strategy by focusing on “hard security problems” detecting events such as lateral movement, side-channel data escapes, insider issues, or stealthy behavior in general.  

 
### **Telemetry**
* Flows.
* DNS (pcaps).

### **Parallel Ingest Framework**
* Open source decoders.
* Load data in Hadoop.
* Data transformation.


### **Machine Learning**
* Filter billion of events to a few thousands.
* Unsupervised learning.

### **Operational Analytics**
* Visualization.
* Attack heuristics.
* Noise filter.

## Try the ONI UI with example data:

*Running Demo on Docker*

1. [Install Docker](https://docs.docker.com/engine/installation/) for your platform 
2. Run the container: `docker run -it -p 8889:8889 opennetworkinsight/oni-demo`
3. visit [http://localhost:8889/files/ui/flow/suspicious.html#date=2016-07-08](http://localhost:8889/files/ui/proxy/suspicious.html#date=2016-07-08) in your browser to get started

For the full instructions visit the [opennetworkinsight](https://hub.docker.com/r/opennetworkinsight/oni-demo/) on Docker hub

*Running the standalone demo*

pull the code from [oni-demo repo](https://github.com/Open-Network-Insight/oni-demo) and install it yourself

## **Getting Started**

ONI can be installed by following our installation manual. To get started, [check out the installation instructions in the documentation](https://github.com/Open-Network-Insight/open-network-insight/wiki).

## Branches

**stable release**

* master

**latest**

* dev

the **master** branch will always be the latest stable release, while current code development will be done in **dev**

**warning** the dev branch is not recommended for a production environment and should only be used for testing

## If you want all of the ONI code at once, just clone it!
        git clone --recursive https://github.com/Open-Network-Insight/open-network-insight.git

## **Roadmap**

TBD

## **Documentation (Developer Guide)**

ONI functionality is divided into different repositories, go to each repository for developer documentation:

* [oni-ingest](https://github.com/Open-Network-Insight/oni-ingest)
* [oni-ml](https://github.com/Open-Network-Insight/oni-ml)
* [oni-oa](https://github.com/Open-Network-Insight/oni-oa)
* [oni-setup](https://github.com/Open-Network-Insight/oni-setup)
* [oni-nfdump](https://github.com/Open-Network-Insight/oni-nfdump)
* [oni-lda-c](https://github.com/Open-Network-Insight/oni-lda-c)

## **Community Support**

Our Central repository for our Open Network Insight solution is found here. If you find a bug, have question or something to discuss please contact us:

* [Create an Issue](https://github.com/Open-Network-Insight/open-network-insight/issues). For issue creation guidelines and how to create an issue for ONI please go to: [How to create an Issue](ISSUES.md)
* [Go to our Slack channel](https://opennetworkinsights.slack.com/messages/general/). 

## **Contributing to ONI**

Help us improve ONI!

ONI is Apache 2.0 licensed and accepts contributions via GitHub pull requests. Please follow the next steps
and join our comunity.

### **Contribuiting to ONI code**

* Fork the repo of the module that you wish to commit to.
* Create a Branch, we use [topic branches](https://git-scm.com/book/en/v2/Git-Branching-Branching-Workflows#Topic-Branches) for our commits. 
* Push your commit(s) to your repository.
* Create a pull request to the original repo in ONI organization.

### **Commit Guidelines**

* Bug fixes should be a single commit.
* Please be clear with the commit messages about what you are fixing or adding to the code base. If you code is addressing an open issue please add the reference to the issue in the comments with: Fix: Issue's URL. 


### **Merge approval**

ONI maintainers use LGTM (Looks Good to Me) in a comments on the code review to indicate acceptance, 
at least 3 "LGTM" from maintainers are required to approve the merge. If you have any question or concern please feel free to add a comment in your pull request or branch and tag any of the maintainers.


## **Licensing**

ONI is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

* oni-nfdump [License](https://github.com/Open-Network-Insight/oni-nfdump/blob/master/BSD-license.txt)
* oni-lda-c [License](https://github.com/Open-Network-Insight/oni-lda-c/blob/1.0.1/license.txt)

## **Maintainers**

<table border="0" cellspacing="0" cellpadding="0">
        <tr>
                <td width="125"><a href="https://github.com/EverLoSa"><sub>@EverLoSa</sub><img src="https://avatars.githubusercontent.com/EverLosa" alt="@EverLoSa"></a></td>
                <td width="125"><a href="https://github.com/ledalima"><sub>@ledalima</sub><img src="https://avatars.githubusercontent.com/ledalima" alt="@ledalima"></a></td>
                <td width="125"><a href="https://github.com/rabarona "><sub>@rabarona</sub><img src="https://avatars.githubusercontent.com/rabarona " alt="@rabarona"></a></td>
                <td width="125"><a href="https://github.com/daortizh "><sub>@daortizh</sub><img src="https://avatars.githubusercontent.com/daortizh " alt="@daortizh"></a></td>   	
        </tr>
        <tr> 
                <td width="125"><a href="https://github.com/natedogs911 "><sub>@natedogs911</sub><img src="https://avatars.githubusercontent.com/natedogs911" alt="@natedogs911"></a></td>
                <td width="125"><a href="https://github.com/NathanSegerlind "><sub>@NathanSegerlind </sub><img src="https://avatars.githubusercontent.com/NathanSegerlind " alt="@NathanSegerlind"></a></td>
                <td width="125"><a href="https://github.com/moy8011"><sub>@moy8011</sub><img src="https://avatars.githubusercontent.com/moy8011" alt="@moy8011"></a></td>
        </tr>
</table>


## Thanks

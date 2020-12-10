# Introduction

Console App to send messages to an Event Hub.

## Requirements

* Visual Studio Code or Visual Studio
* .NET Core 3.1 SDK

## Setup

You can either open the project in Visual Studio code or download the `Publish.zip`, which a pre-compiled version of the application and then follow the instructions in the Execution section of this document.

For the Console App to run you need access to an Event Hub in Azure and the Event Hub Connection String, e.g. `Endpoint=sb://EVENT_HUB_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=SAS_NAME;SharedAccessKey=SAS_KEY;`

## Execution

### Visual Studio Code

Open the folder in Visual Studio Code and at the terminal prompt enter `dotnet run --quantity 200 --interval 1 --duration 600 --event-hub-connection-string "EVENT_HUB_CONNECTION_STRING`.

This will send 200 messages every 1 second for 600 seconds.

### Terminal

Dowload the `Publish.zip` file and unzip it to directory.

Open the directory in your favorite terminal application and run `.\ConsoleApp.exe --quantity 200 --interval 1 --duration 600 --event-hub-connection-string "EVENT_HUB_CONNECTION_STRING`.

## Notes

I did try getting this to work with .NET 5.0 but ran into an isse with `CreateBatchOptions` method of the `EventHubProducerClient` client. Kept throwing an exception.

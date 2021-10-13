#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["linklives-lifecourse-indexer/linklives-lifecourse-indexer.csproj", "linklives-lifecourse-indexer/"]
COPY ["linklives-indexer-Lib/linklives-indexer-lib.csproj", "linklives-indexer-Lib/"]
RUN dotnet restore "linklives-lifecourse-indexer/linklives-lifecourse-indexer.csproj"
COPY . .
WORKDIR "/src/linklives-lifecourse-indexer"
RUN dotnet build "linklives-lifecourse-indexer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "linklives-lifecourse-indexer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "linklives-lifecourse-indexer.dll"]
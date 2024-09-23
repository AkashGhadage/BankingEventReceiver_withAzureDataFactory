﻿// <auto-generated />
using System;
using BankingApi.EventReceiver;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

#nullable disable

namespace BankingApi.EventReceiver.Migrations
{
    [DbContext(typeof(BankingApiDbContext))]
    partial class BankingApiDbContextModelSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("ProductVersion", "8.0.8")
                .HasAnnotation("Relational:MaxIdentifierLength", 128);

            SqlServerModelBuilderExtensions.UseIdentityColumns(modelBuilder);

            modelBuilder.Entity("BankingApi.EventReceiver.BankAccount", b =>
                {
                    b.Property<Guid>("Id")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("uniqueidentifier");

                    b.Property<decimal>("Balance")
                        .HasColumnType("decimal(18,2)");

                    b.HasKey("Id");

                    b.ToTable("BankAccounts");
                });
#pragma warning restore 612, 618
        }
    }
}
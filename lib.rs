// Whitespaces and decimal precisions -- ...
//  ...(up to four places past the decimal) must be accepted by your program

// input:
//
//  * The client ID will be unique per client though are not guaranteed to be ordered.
//  * Transactions to the client account 2 could occur before transactions to the client account 1.
//  * Likewise, transaction IDs (tx) are globally unique, though are also not guaranteed to be ordered.
//  * You can assume the transactions occur chronologically in the file, so if transaction b appears after a in the input file
//    then you can assume b occurred chronologically after a.
//  * Whitespaces and decimal precisions
//    (up to four places past the decimal) must be accepted by your program.

// assumptions:
//
// i. money system starts at total money = 0;
// ii. money system has no accounts
// iii. clients can only deposit into their accounts
//
//
// questions:
//
// (i) what happens when our engine falls over mid-processing?
// (ii) how do we know if a account already exists?
// (iii) if an account exists where is it sharded?
//

mod account;
mod io;
mod manager;
mod output;
mod shard;
mod transaction;

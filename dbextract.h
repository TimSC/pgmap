#ifndef PGMAP_DBEXTRACT_H
#define PGMAP_DBEXTRACT_H
#include <pqxx/pqxx>
#include <string>
#include <cstdint>
#include <memory>

// Resumable export. The caller keeps its transaction alive until completion.
class PgExtractExport
{
    friend class PgTransaction;
    std::shared_ptr<pqxx::connection> connection;
    std::shared_ptr<class PgWork> work;
    std::shared_ptr<class IDataStreamHandler> output;
    std::string prefix;
    int64_t extractId, lastId = 0;
    int phase = 0;
    PgExtractExport(std::shared_ptr<pqxx::connection>, std::shared_ptr<class PgWork>,
        const std::string &, int64_t, const std::string &,
        std::shared_ptr<class IDataStreamHandler>);
public:
    int64_t GetId() const { return extractId; }
    // Encode up to 1000 objects; return 1 when the XML document is complete.
    int Continue();
};

// Caller holds main-map locks followed by EXCLUSIVE locks on all extract tables.
// Updates to the latest visible source snapshot only. Returns the extract ID.
// Throws on unsupported activity; caller must abort the transaction on failure.
int64_t DbUpdateExtractNodes(pqxx::connection &connection, pqxx::transaction_base &work,
	const std::string &prefix, int64_t extractId, const std::string &name);
#endif
